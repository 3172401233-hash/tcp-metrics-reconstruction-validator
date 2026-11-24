import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TcpConnectionTracker, seqDiff, SEQ_SPACE } from '../index.js';

function track(events, options) {
  return new TcpConnectionTracker(options).ingestAll(events).finalize();
}

test('seqDiff 处理 mod 2^32 有符号差值与回绕', () => {
  assert.equal(seqDiff(200, 100), 100);
  assert.equal(seqDiff(100, 200), -100);
  assert.equal(seqDiff(10, SEQ_SPACE - 10), 20);
  assert.equal(seqDiff(SEQ_SPACE - 10, 10), -20);
});

test('丢包恢复：3 个重复 ACK 触发快速重传，RTT 遵循 Karn 算法', () => {
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 50, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'send', ts: 110, seq: 1100, len: 100 },
    { type: 'send', ts: 120, seq: 1200, len: 100 },
    { type: 'ack', ts: 160, ack: 1100 },
    { type: 'ack', ts: 170, ack: 1100 },
    { type: 'ack', ts: 180, ack: 1100 },
    { type: 'ack', ts: 190, ack: 1100 },
    { type: 'retransmit', ts: 195, seq: 1100, len: 100 },
    { type: 'ack', ts: 260, ack: 1300 },
  ]);

  assert.equal(report.phases.handshake.rtt.samples, 1);
  assert.equal(report.phases.handshake.rtt.avgMs, 50);

  // 数据阶段：段 1000 采样 60ms；段 1100 被重传不采样；段 1200 采样 140ms。
  assert.equal(report.phases.data.rtt.samples, 2);
  assert.equal(report.phases.data.rtt.minMs, 60);
  assert.equal(report.phases.data.rtt.maxMs, 140);

  assert.equal(report.retransmits.fast, 1);
  assert.equal(report.retransmits.timeout, 0);
  assert.equal(report.retransmits.effective, 1);
  assert.equal(report.retransmits.effectiveRate, 1 / 3);
  assert.equal(report.acks.maxDupAcks, 3);
  assert.equal(report.bytes.unacked, 0);
});

test('无重复 ACK 时的重传判定为超时重传', () => {
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 5000, len: 200 },
    { type: 'retransmit', ts: 500, seq: 5000, len: 200 },
    { type: 'ack', ts: 560, ack: 5200 },
  ]);

  assert.equal(report.retransmits.timeout, 1);
  assert.equal(report.retransmits.fast, 0);
  assert.equal(report.retransmits.effective, 1);
  // 重传段不产出 RTT 样本。
  assert.equal(report.phases.data.rtt.samples, 0);
  assert.equal(report.bytes.unacked, 0);
});

test('伪重传：目标字节已被 ACK 或 SACK 覆盖', () => {
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'send', ts: 110, seq: 1100, len: 100 },
    { type: 'send', ts: 120, seq: 1200, len: 100 },
    // 延迟的 ACK 与重传在途交叉：ACK 已覆盖 1000-1100。
    { type: 'ack', ts: 200, ack: 1100, sack: [{ left: 1200, right: 1300 }] },
    { type: 'retransmit', ts: 210, seq: 1000, len: 100 },
    { type: 'retransmit', ts: 220, seq: 1200, len: 100 },
    // 1100-1200 未被覆盖，是真重传（无重复 ACK，判为超时）。
    { type: 'retransmit', ts: 230, seq: 1100, len: 100 },
    { type: 'ack', ts: 300, ack: 1300 },
  ]);

  assert.equal(report.retransmits.spurious, 2);
  assert.equal(report.retransmits.timeout, 1);
  assert.equal(report.retransmits.effective, 1);
  assert.equal(report.anomalies.spuriousRetransmits, 2);
  assert.equal(report.bytes.sacked, 100);
});

test('零窗口：统计持续时长与探测次数', () => {
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'ack', ts: 150, ack: 1100, window: 0 },
    { type: 'zeroWindowProbe', ts: 300 },
    { type: 'zeroWindowProbe', ts: 500 },
    { type: 'window', ts: 600, window: 4096 },
    { type: 'send', ts: 610, seq: 1100, len: 100 },
    { type: 'ack', ts: 660, ack: 1200, window: 4096 },
  ]);

  assert.equal(report.zeroWindow.episodes, 1);
  assert.equal(report.zeroWindow.totalDurationMs, 450);
  assert.equal(report.zeroWindow.probes, 2);
  assert.equal(report.bytes.unacked, 0);
});

test('重复 ACK 计数与完全重复事件去重', () => {
  const dupAck = { type: 'ack', ts: 170, ack: 1100 };
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'ack', ts: 150, ack: 1100 },
    dupAck,
    { ...dupAck },
    { type: 'ack', ts: 180, ack: 1100 },
    { type: 'ack', ts: 190, ack: 1100 },
  ]);

  // 完全相同的 dupAck 事件只计一次，重复 ACK 总数为 3。
  assert.equal(report.anomalies.duplicateEvents, 1);
  assert.equal(report.acks.maxDupAcks, 3);
});

test('乱序样本：finalize 按时间戳重排并计入异常', () => {
  const report = track([
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'ack', ts: 160, ack: 1100 },
    { type: 'state', ts: 50, state: 'ESTABLISHED' },
  ]);

  assert.equal(report.anomalies.outOfOrderEvents, 2);
  assert.equal(report.phases.handshake.rtt.avgMs, 50);
  assert.equal(report.phases.data.rtt.samples, 1);
  assert.equal(report.phases.data.rtt.avgMs, 60);
  assert.deepEqual(
    report.timeline.filter((e) => e.type === 'state').map((e) => e.ts),
    [0, 50],
  );
});

test('序列号回绕：跨 2^32 边界的发送与确认', () => {
  const base = SEQ_SPACE - 50;
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: base, len: 100 },
    { type: 'ack', ts: 180, ack: 50 },
    { type: 'send', ts: 200, seq: 50, len: 30 },
    { type: 'ack', ts: 260, ack: 80 },
  ]);

  assert.equal(report.anomalies.seqWraps, 1);
  assert.equal(report.bytes.sent, 130);
  assert.equal(report.bytes.unacked, 0);
  assert.equal(report.phases.data.rtt.samples, 2);
  assert.equal(report.phases.data.rtt.avgMs, 70);
});

test('未确认字节随发送与 ACK 推进变化', () => {
  const tracker = new TcpConnectionTracker();
  tracker.ingestAll([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 300 },
    { type: 'send', ts: 110, seq: 1300, len: 200 },
    { type: 'ack', ts: 150, ack: 1300 },
  ]);
  const report = tracker.finalize();
  assert.equal(report.bytes.sent, 500);
  assert.equal(report.bytes.unacked, 200);
});

test('连接关闭进入 close 阶段并记录时间线', () => {
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'ack', ts: 150, ack: 1100 },
    { type: 'state', ts: 200, state: 'FIN_WAIT_1' },
    { type: 'state', ts: 240, state: 'TIME_WAIT' },
    { type: 'close', ts: 300, reason: 'fin' },
  ]);

  assert.equal(report.state, 'TIME_WAIT');
  assert.deepEqual(
    report.timeline.map((e) => e.type),
    ['state', 'state', 'state', 'state', 'close'],
  );
  assert.equal(report.timeline.at(-1).detail, 'fin');
});

test('重复的重传事件被去重', () => {
  const report = track([
    { type: 'state', ts: 0, state: 'SYN_SENT' },
    { type: 'state', ts: 40, state: 'ESTABLISHED' },
    { type: 'send', ts: 100, seq: 1000, len: 100 },
    { type: 'retransmit', ts: 400, seq: 1000, len: 100 },
    { type: 'retransmit', ts: 800, seq: 1000, len: 100 },
  ]);

  assert.equal(report.retransmits.timeout, 1);
  assert.equal(report.anomalies.duplicateRetransmitEvents, 1);
});
