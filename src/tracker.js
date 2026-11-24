/**
 * TCP 连接指标重建器。
 *
 * 输入稀疏的监控事件（状态变迁、发送、ACK/SACK、重传、窗口、关闭），
 * 重建分阶段 RTT、有效重传、未确认字节与连接时间线。
 *
 * 设计要点：
 * - 序列号按 mod 2^32 有符号差值比较，天然支持回绕。
 * - RTT 采样遵循 Karn 算法：被重传过的段不参与采样。
 * - 重传分类：快速重传（>=3 个重复 ACK 触发）、超时重传、
 *   伪重传（目标字节已被 ACK 或 SACK 覆盖）。
 * - 完全重复的输入事件被去重；乱序事件在 finalize 时按时间戳稳定排序。
 */

export const SEQ_SPACE = 2 ** 32;

/** mod 2^32 有符号差值 (a - b)，结果落在 [-2^31, 2^31)。 */
export function seqDiff(a, b) {
  let d = (a - b) % SEQ_SPACE;
  if (d >= SEQ_SPACE / 2) d -= SEQ_SPACE;
  if (d < -SEQ_SPACE / 2) d += SEQ_SPACE;
  return d;
}

const CLOSE_STATES = new Set([
  'FIN_WAIT_1',
  'FIN_WAIT_2',
  'CLOSING',
  'TIME_WAIT',
  'LAST_ACK',
  'CLOSE_WAIT',
  'CLOSED',
]);

class RttStats {
  constructor() {
    this.samples = 0;
    this.min = Infinity;
    this.max = -Infinity;
    this.total = 0;
  }

  add(value) {
    this.samples += 1;
    this.total += value;
    if (value < this.min) this.min = value;
    if (value > this.max) this.max = value;
  }

  toJSON() {
    return {
      samples: this.samples,
      minMs: this.samples ? this.min : null,
      maxMs: this.samples ? this.max : null,
      avgMs: this.samples ? this.total / this.samples : null,
    };
  }
}

export class TcpConnectionTracker {
  /**
   * @param {object} [options]
   * @param {number} [options.fastRetransmitThreshold=3] 触发快速重传的重复 ACK 数
   */
  constructor(options = {}) {
    this.fastRetransmitThreshold = options.fastRetransmitThreshold ?? 3;
    this._events = [];
    this._lastIngestTs = -Infinity;
    this._outOfOrder = 0;
  }

  /** 摄入一条事件；乱序事件会被记录并在 finalize 时重排。 */
  ingest(event) {
    if (!event || typeof event.ts !== 'number' || !event.type) {
      throw new TypeError('事件必须包含 type 与数值型 ts 字段');
    }
    if (event.ts < this._lastIngestTs) this._outOfOrder += 1;
    this._lastIngestTs = Math.max(this._lastIngestTs, event.ts);
    this._events.push(event);
    return this;
  }

  ingestAll(events) {
    for (const event of events) this.ingest(event);
    return this;
  }

  /** 处理全部事件并输出重建报告。 */
  finalize() {
    const sorted = [...this._events].sort((a, b) => a.ts - b.ts);
    const conn = new ConnectionState(this.fastRetransmitThreshold);
    for (const event of sorted) conn.apply(event);
    const report = conn.report();
    report.anomalies.outOfOrderEvents = this._outOfOrder;
    return report;
  }
}

class ConnectionState {
  constructor(fastThreshold) {
    this.fastThreshold = fastThreshold;

    this.state = 'INIT';
    this.phase = 'handshake';
    this.synSentTs = null;
    this.establishedTs = null;
    this.closedTs = null;

    // 序列号锚点：把 mod 2^32 的原始序号映射到单调的绝对坐标。
    this.anchorRaw = null;
    this.anchorAbs = 0;
    this.lastRawSeq = null;

    this.inFlight = []; // { seqAbs, endAbs, ts, retransmitted }
    this.maxSentAbs = 0;
    this.ackAbs = 0;
    this.hasAck = false;
    this.sackBlocks = []; // { left, right } 绝对坐标

    this.dupAckCount = 0;
    this.maxDupAcks = 0;

    this.bytesSent = 0;
    this.sendSegments = 0;

    this.retransmits = { fast: 0, timeout: 0, spurious: 0 };
    this.seenRetransmits = new Set();

    this.zeroWindow = { open: false, openTs: null, episodes: 0, totalMs: 0, probes: 0 };

    this.rtt = { handshake: new RttStats(), data: new RttStats(), close: new RttStats() };

    this.timeline = [];
    this.anomalies = {
      duplicateEvents: 0,
      duplicateRetransmitEvents: 0,
      seqWraps: 0,
      spuriousRetransmits: 0,
      outOfOrderEvents: 0,
    };
    this.seenEventKeys = new Set();
  }

  /** 原始序号 -> 绝对坐标（相对首个观察到的序号）。 */
  toAbs(raw) {
    if (this.anchorRaw === null) {
      this.anchorRaw = raw;
      this.anchorAbs = 0;
      return 0;
    }
    return this.anchorAbs + seqDiff(raw, this.anchorRaw);
  }

  advanceAnchor(raw, abs) {
    if (abs >= this.anchorAbs) {
      this.anchorRaw = raw;
      this.anchorAbs = abs;
    }
  }

  apply(event) {
    const key = JSON.stringify(event);
    if (this.seenEventKeys.has(key)) {
      this.anomalies.duplicateEvents += 1;
      return;
    }
    this.seenEventKeys.add(key);

    switch (event.type) {
      case 'state':
        return this.onState(event);
      case 'send':
        return this.onSend(event);
      case 'ack':
        return this.onAck(event);
      case 'retransmit':
        return this.onRetransmit(event);
      case 'window':
        return this.onWindow(event);
      case 'zeroWindowProbe':
        return this.onZeroWindowProbe(event);
      case 'close':
        return this.onClose(event);
      default:
        throw new TypeError(`未知事件类型: ${event.type}`);
    }
  }

  onState({ ts, state }) {
    const prev = this.state;
    this.state = state;
    if (state === 'SYN_SENT') this.synSentTs = ts;
    if (state === 'ESTABLISHED' && this.phase === 'handshake') {
      this.establishedTs = ts;
      if (this.synSentTs !== null) {
        this.rtt.handshake.add(ts - this.synSentTs);
      }
      this.phase = 'data';
    }
    if (CLOSE_STATES.has(state)) this.phase = 'close';
    this.timeline.push({ ts, type: 'state', detail: `${prev} -> ${state}` });
  }

  onSend({ ts, seq, len = 0 }) {
    if (this.lastRawSeq !== null && seqDiff(seq, this.lastRawSeq) > 0 && seq < this.lastRawSeq) {
      this.anomalies.seqWraps += 1;
    }
    this.lastRawSeq = seq;

    const seqAbs = this.toAbs(seq);
    const endAbs = seqAbs + len;
    this.advanceAnchor(seq, seqAbs);
    this.inFlight.push({ seqAbs, endAbs, ts, retransmitted: false });
    this.maxSentAbs = Math.max(this.maxSentAbs, endAbs);
    this.bytesSent += len;
    this.sendSegments += 1;
  }

  onAck({ ts, ack, sack = [], window }) {
    const ackAbs = this.toAbs(ack);
    this.advanceAnchor(ack, ackAbs);

    if (this.hasAck && ackAbs === this.ackAbs && sack.length === 0) {
      this.dupAckCount += 1;
      this.maxDupAcks = Math.max(this.maxDupAcks, this.dupAckCount);
    } else {
      this.dupAckCount = 0;
    }
    this.hasAck = true;
    this.ackAbs = Math.max(this.ackAbs, ackAbs);

    for (const block of sack) {
      const left = this.toAbs(block.left);
      const right = this.toAbs(block.right);
      this.sackBlocks.push({ left, right });
    }

    // Karn 算法：仅对从未重传、且被本次 ACK 完全覆盖的段采样 RTT。
    const remaining = [];
    for (const seg of this.inFlight) {
      if (seg.endAbs <= this.ackAbs) {
        if (!seg.retransmitted) {
          this.rtt[this.phase].add(ts - seg.ts);
        }
      } else {
        remaining.push(seg);
      }
    }
    this.inFlight = remaining;

    if (typeof window === 'number') this.updateWindow(ts, window);
  }

  isSacked(seqAbs, endAbs) {
    return this.sackBlocks.some((b) => b.left <= seqAbs && endAbs <= b.right);
  }

  onRetransmit({ ts, seq, len = 0 }) {
    const seqAbs = this.toAbs(seq);
    const endAbs = seqAbs + len;

    const dedupeKey = `${seqAbs}:${endAbs}`;
    if (this.seenRetransmits.has(dedupeKey)) {
      this.anomalies.duplicateRetransmitEvents += 1;
      return;
    }
    this.seenRetransmits.add(dedupeKey);

    let kind;
    if (endAbs <= this.ackAbs || this.isSacked(seqAbs, endAbs)) {
      kind = 'spurious';
      this.retransmits.spurious += 1;
      this.anomalies.spuriousRetransmits += 1;
    } else if (this.dupAckCount >= this.fastThreshold) {
      kind = 'fast';
      this.retransmits.fast += 1;
    } else {
      kind = 'timeout';
      this.retransmits.timeout += 1;
    }

    // 被重传的在途段此后不再参与 RTT 采样（Karn）。
    for (const seg of this.inFlight) {
      if (seg.seqAbs >= seqAbs && seg.seqAbs < endAbs) seg.retransmitted = true;
    }

    this.timeline.push({ ts, type: 'retransmit', detail: kind, seq, len });
  }

  onWindow({ ts, window }) {
    this.updateWindow(ts, window);
  }

  updateWindow(ts, window) {
    if (window === 0 && !this.zeroWindow.open) {
      this.zeroWindow.open = true;
      this.zeroWindow.openTs = ts;
      this.zeroWindow.episodes += 1;
      this.timeline.push({ ts, type: 'zeroWindowOpen' });
    } else if (window > 0 && this.zeroWindow.open) {
      this.zeroWindow.open = false;
      this.zeroWindow.totalMs += ts - this.zeroWindow.openTs;
      this.timeline.push({ ts, type: 'zeroWindowClose', detail: `持续 ${ts - this.zeroWindow.openTs}ms` });
    }
  }

  onZeroWindowProbe({ ts }) {
    this.zeroWindow.probes += 1;
    this.timeline.push({ ts, type: 'zeroWindowProbe' });
  }

  onClose({ ts, reason = 'unknown' }) {
    this.phase = 'close';
    this.closedTs = ts;
    if (this.zeroWindow.open) {
      this.zeroWindow.open = false;
      this.zeroWindow.totalMs += ts - this.zeroWindow.openTs;
    }
    this.timeline.push({ ts, type: 'close', detail: reason });
  }

  report() {
    const { fast, timeout, spurious } = this.retransmits;
    const effective = fast + timeout;
    const total = fast + timeout + spurious;
    const unackedBytes = Math.max(0, this.maxSentAbs - this.ackAbs);
    const sackedBytes = this.sackBlocks.reduce((sum, b) => sum + Math.max(0, b.right - b.left), 0);

    return {
      state: this.state,
      phases: {
        handshake: { rtt: this.rtt.handshake.toJSON() },
        data: { rtt: this.rtt.data.toJSON() },
        close: { rtt: this.rtt.close.toJSON() },
      },
      retransmits: {
        total,
        fast,
        timeout,
        spurious,
        effective,
        effectiveRate: this.sendSegments ? effective / this.sendSegments : 0,
      },
      bytes: {
        sent: this.bytesSent,
        unacked: unackedBytes,
        sacked: sackedBytes,
      },
      acks: { maxDupAcks: this.maxDupAcks },
      zeroWindow: {
        episodes: this.zeroWindow.episodes,
        totalDurationMs: this.zeroWindow.totalMs,
        probes: this.zeroWindow.probes,
      },
      anomalies: { ...this.anomalies },
      timeline: this.timeline,
    };
  }
}
