'use strict';

const Base = require('sdk-base');
const bsInsert = require('binary-search-insert');
const comparator = (a, b) => a.offset - b.offset;

const pullMaxIdleTime = 120000;

class ProcessQueue extends Base {
  constructor(options = {}) {
    super(options);

    this.msgList = [];
    this.droped = false;
    this.lastPullTimestamp = Date.now();
    this.lastConsumeTimestamp = Date.now();

    this.locked = false;
    this.lastLockTimestamp = Date.now();
  }

  get maxSpan() {
    const msgCount = this.msgCount;
    if (msgCount) {
      return this.msgList[msgCount - 1].queueOffset - this.msgList[0].queueOffset;
    }
    return 0;
  }

  get msgCount() {
    return this.msgList.length;
  }

  get isPullExpired() {
    return Date.now() - this.lastPullTimestamp > pullMaxIdleTime;
  }

  putMessage(msgs) {
    for (const msg of msgs) {
      bsInsert(this.msgList, comparator, msg);
    }
    this.queueOffsetMax = this.msgList[this.msgCount - 1].queueOffset;
  }

  remove(count = 1) {
    this.msgList.splice(0, count);
    return this.msgCount ? this.msgList[0].queueOffset : this.queueOffsetMax + 1;
  }

  clear() {
    this.msgList = [];
  }
}

module.exports = ProcessQueue;
