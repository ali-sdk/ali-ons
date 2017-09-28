'use strict';

const fs = require('fs');
const path = require('path');
const osenv = require('osenv');
const mkdirp = require('mkdirp');
const is = require('is-type-of');
const Base = require('sdk-base');
const ReadOffsetType = require('./read_offset_type');
const localOffsetStoreDir = path.join(osenv.home(), '.rocketmq_offsets_node');

class LocalFileOffsetStore extends Base {

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
    this.storePath = path.join(localOffsetStoreDir, mqClient.clientId, groupName, 'offsets.json');

    mkdirp.sync(path.dirname(this.storePath));
  }

  get logger() {
    return this.mqClient.logger;
  }

  /**
   * 加载Offset
   */
  * load() {
    const data = yield this.readLocalOffset();

    if (data && data.offsetTable) {
      for (const key in data.offsetTable) {
        if (is.number(data.offsetTable[key])) {
          this.offsetTable.set(key, data.offsetTable[key]);
        }
      }
    }
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
    if (increaseOnly || prev < offset) {
      this.offsetTable.set(messageQueue.key, offset);
    }
    this.logger.info('[mq:LocalFileOffsetStore] update offset for messageQueue: %s, current offset: %d, prev offset: %s, increaseOnly: %s', messageQueue.key, offset, prev, increaseOnly);
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
      {
        const offset = this.offsetTable.get(messageQueue.key);
        if (!is.nullOrUndefined(offset)) {
          return offset;
        }
        break;
      }
      case ReadOffsetType.READ_FROM_STORE:
      {
        const data = yield this.readLocalOffset();
        if (data && data.offsetTable) {
          const offset = data.offsetTable[messageQueue.key];
          if (is.number(offset)) {
            this.updateOffset(messageQueue, offset, false);
            return offset;
          }
        }
        break;
      }
      default:
        break;
    }
    return -1;
  }

  /**
   * 持久化全部消费进度，可能持久化本地或者远端Broker
   * @param {Array} mqs -
   * @return {void}
   */
  * persistAll(mqs) {
    if (mqs && mqs.length) {
      const data = {
        offsetTable: {},
      };

      const newTable = new Map();
      for (const messageQueue of mqs) {
        const offset = this.offsetTable.get(messageQueue.key) || -1;
        data.offsetTable[messageQueue.key] = offset;
        newTable.set(messageQueue.key, offset);
      }
      this.offsetTable = newTable;

      fs.writeFileSync(this.storePath, JSON.stringify(data));
    }
  }

  /* eslint-disable no-empty-function */
  /**
   * 持久化消费进度
   * @return {void}
   */
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

  /**
   * 从本地文件读消费进度
   * @return {void}
   */
  * readLocalOffset() {
    if (!fs.existsSync(this.storePath)) {
      return null;
    }

    const content = fs.readFileSync(this.storePath);
    if (!content) {
      return null;
    }
    try {
      return JSON.parse(content.toString());
    } catch (e) {
      e.message = 'readLocalOffset() failed' + e.message;
      this.logger.error(e);
      return null;
    }
  }
}

module.exports = LocalFileOffsetStore;
