'use strict';

const is = require('is-type-of');
const Base = require('sdk-base');
const ReadOffsetType = require('./read_offset_type');

class LocalMemoryOffsetStore extends Base {
  /**
   * 消费进度存储到Consumer本地
   * @param {MQClient} mqClient -
   * @param {String} groupName 分组名
   * @contructor
   */
  constructor(mqClient, groupName) {
    super();
    this.mqClient = mqClient;
    this.groupName = groupName;
    this.offsetTable = new Map();
  }

  get logger() {
    return this.mqClient.logger;
  }

  /**
   * 更新消费进度，存储到内存
   * @param {MessageQueue} messageQueue -
   * @param {Number} offset 新的进度
   * @param {Boolean} increaseOnly 是否强制覆盖
   * @return {void}
   */
  updateOffset(messageQueue, offset, increaseOnly) {
    increaseOnly = increaseOnly || false;
    if (!messageQueue) {
      return;
    }

    let prev = -1;
    if (this.offsetTable.has(messageQueue.key)) {
      prev = this.offsetTable.get(messageQueue.key);
    }
    if (prev >= offset && increaseOnly) {
      return;
    }
    this.offsetTable.set(messageQueue.key, offset);
    this.logger.info('[mq:LocalMemoryOffsetStore] update offset for messageQueue: %s, current offset: %d, prev offset: %s, increaseOnly: %s', messageQueue.key, offset, prev, increaseOnly);
  }

  /**
   * 从本地缓存读取消费进度
   * @param {MessageQueue} messageQueue -
   * @param {String} type 消费类型（从哪里开始消费）
   * @param {Function} callback 回调函数
   * @return {void}
   */
  * readOffset(messageQueue, type) {
    if (!messageQueue) {
      return -1;
    }
    switch (type) {
      case ReadOffsetType.MEMORY_FIRST_THEN_STORE:
      case ReadOffsetType.READ_FROM_MEMORY:
      case ReadOffsetType.READ_FROM_STORE:
      {
        const offset = this.offsetTable.get(messageQueue.key);
        if (!is.nullOrUndefined(offset)) {
          return offset;
        }
        break;
      }
      default:
        break;
    }
    return -1;
  }

  /* eslint-disable no-empty-function */

  * load() {}

  * persistAll() {}

  * persist() {}

  /* eslint-enable no-empty-function */

  /**
   * 删除不必要的MessageQueue offset
   * @param {Object} messageQueue 消息队列
   * @return {void}
   */
  removeOffset(messageQueue) {
    if (messageQueue) {
      this.offsetTable.delete(messageQueue.key);
    }
  }
}

module.exports = LocalMemoryOffsetStore;
