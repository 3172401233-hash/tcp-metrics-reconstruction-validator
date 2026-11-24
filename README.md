# tcp-metrics-reconstruction-validator
从丢包和重传事件重建 TCP RTT 与连接指标

## 定位与边界

监控探针往往只能采到少量丢包与重传事件，本工具基于这些**稀疏事件**重建
连接级指标：分阶段 RTT、有效重传率、未确认字节与连接时间线。

**本工具不抓取真实报文。** 它不依赖 pcap、不监听网卡，只消费外部监控系统
（探针、日志、eBPF 采样等）上报的事件流，因此对重复 ACK、SACK 与零窗口
探测做了专门处理，避免简单计数器得出错误结论。

## 运行环境

- Node.js >= 24（开发验证版本 24.16.0）

```bash
node --test   # 执行全部测试
```

## 输入事件

通过 `ingest` / `ingestAll` 逐条喂入，时间戳单位毫秒：

| type              | 字段                                   | 说明                     |
| ----------------- | -------------------------------------- | ------------------------ |
| `state`           | `ts, state`                            | TCP 状态变迁             |
| `send`            | `ts, seq, len`                         | 发出数据段               |
| `ack`             | `ts, ack, sack?, window?`              | 累计 ACK / SACK 块 / 窗口 |
| `retransmit`      | `ts, seq, len`                         | 观测到重传               |
| `window`          | `ts, window`                           | 通告窗口更新             |
| `zeroWindowProbe` | `ts`                                   | 零窗口探测               |
| `close`           | `ts, reason`                           | 连接关闭                 |

## 输出指标

`finalize()` 返回：

- `phases.{handshake,data,close}.rtt`：分阶段 RTT 样本数 / 最小 / 最大 / 均值，
  遵循 Karn 算法，被重传的段不参与采样。
- `retransmits`：区分 `fast`（≥3 个重复 ACK 触发）、`timeout`、`spurious`
  （目标字节已被 ACK 或 SACK 覆盖的伪重传），`effective = fast + timeout`，
  `effectiveRate = effective / 发送段数`。
- `bytes.unacked`：未确认字节（已发送最高序号 − 累计 ACK，支持 2^32 回绕）。
- `zeroWindow`：零窗口次数、累计时长、探测次数。
- `anomalies`：重复事件、重复重传、乱序样本、序号回绕、伪重传计数。
- `timeline`：状态变迁、重传分类、零窗口开合、关闭的时间线。

## 示例

```js
import { TcpConnectionTracker } from './index.js';

const report = new TcpConnectionTracker()
  .ingestAll([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 50, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'ack', ts: 160, ack: 1100 },
    { type: 'close', ts: 300, reason: 'fin' },
  ])
  .finalize();

console.log(report.phases.data.rtt.avgMs); // 60
```

## 关键设计

- **序列号回绕**：所有序号比较使用 mod 2^32 有符号差值（`seqDiff`），
  跨边界的发送与确认不会产生负值或巨大假值。
- **重复事件去重**：完全相同的输入事件只处理一次；相同 ACK 号的多次
  上报仍计入重复 ACK（这是快速重传的触发信号，不能与重复事件混淆）。
- **乱序样本**：摄入时允许时间戳乱序，`finalize` 前稳定排序，乱序条数
  计入 `anomalies.outOfOrderEvents`。
