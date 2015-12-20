'use strict';

const pullMaxIdleTime = 120000;

class ProcessQueue {
  constructor() {
    this.msgs = [];

    // 当前Q是否被rebalance丢弃
    this.droped = false;
    this.lastPullTimestamp = Date.now();
    // 最后一次消费的时间戳
    this.lastConsumeTimestamp = Date.now();

    // 是否从Broker锁定
    this.locked = false;
    // 最后一次锁定成功时间戳
    this.lastLockTimestamp = Date.now();
  }

  get isPullExpired() {
    return Date.now() - this.lastPullTimestamp > pullMaxIdleTime;
  }
  putMessage() {}
  removeMessage() {}
  takeMessags() {}
}

module.exports = ProcessQueue;
