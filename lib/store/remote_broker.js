'use strict';

const is = require('is-type-of');
const gather = require('p-gather');
const Base = require('sdk-base');
const ReadOffsetType = require('./read_offset_type');

class RemoteBrokerOffsetStore extends Base {

  /**
   * consume offset store at remote server
   * @param {MQClient} mqClient - mq client
   * @param {String} groupName - group name
   * @constructor
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

  /* eslint-disable no-empty-function */
  /**
   * load the offset
   * @return {void}
   */
  async load() {}

  /* eslint-enable no-empty-function */

  /**
   * update consume offset
   * @param {MessageQueue} messageQueue - message queue
   * @param {Number} offset - new offset
   * @param {Boolean} increaseOnly - force override
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
    this.logger.info('[mq:RemoteBrokerOffsetStore] update offset for messageQueue: %s, current offset: %d, prev offset: %s, increaseOnly: %s', messageQueue.key, offset, prev, increaseOnly);
  }

  /**
   * read consume offset
   * @param {MessageQueue} messageQueue - message queue
   * @param {String} type - consume from where
   * @return {Number} offset
   */
  async readOffset(messageQueue, type) {
    if (!messageQueue) {
      return -1;
    }

    try {
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
          const brokerOffset = await this.fetchConsumeOffsetFromBroker(messageQueue);
          this.updateOffset(messageQueue, brokerOffset, false);
          return brokerOffset;
        }
        default:
          break;
      }
    } catch (err) {
      err.message = `RemoteBrokerOffsetStore.readOffset failed, topic: ${messageQueue.topic}, group: ${this.groupName}, queueId: ${messageQueue.queueId}, type: ${type} ` + err.message;
      this.emit('error', err);
    }
    return -1;
  }

  /**
   * persist all consume offset
   * @param {Array} mqs - all queues
   * @return {void}
   */
  async persistAll(mqs) {
    if (!mqs || !mqs.length) {
      return;
    }

    const newTable = new Map();
    const tasks = [];
    let info = 'persist all consume offset\n';
    for (const messageQueue of mqs) {
      const offset = this.offsetTable.get(messageQueue.key) || -1;
      newTable.set(messageQueue.key, offset);
      info += `\t- topic="${messageQueue.topic}", brokerName="${messageQueue.brokerName}", queueId="${messageQueue.queueId}", consumer offset => ${offset}\n`;
      tasks.push(this.updateConsumeOffsetToBroker(messageQueue, offset));
    }
    this.logger.info('[mq:RemoteBrokerOffsetStore] %s', info);
    this.offsetTable = newTable;
    const ret = await gather(tasks);
    const errors = ret.filter(item => item.isError);
    for (const err of errors) {
      this.emit('error', err);
    }
  }

  /**
   * persist consume offset
   * @param {MessageQueue} messageQueue - message queue
   * @return {void}
   */
  async persist(messageQueue) {
    const offset = this.offsetTable.get(messageQueue.key);
    this.logger.info('[mq:RemoteBrokerOffsetStore] persist consume offset of [%s], offset: %d', messageQueue.key, offset);
    if (is.number(offset)) {
      await this.updateConsumeOffsetToBroker(messageQueue, offset);
    }
  }

  /**
   * remove message queue
   * @param {MessageQueue} messageQueue - message queue
   * @return {void}
   */
  removeOffset(messageQueue) {
    if (messageQueue) {
      this.offsetTable.delete(messageQueue.key);
    }
  }

  /**
   * fetch consume offset from broker
   * @param {MessageQueue} messageQueue - message queue
   * @return {Number} brokerOffset
   */
  async fetchConsumeOffsetFromBroker(messageQueue) {
    const requestHeader = {
      topic: messageQueue.topic,
      consumerGroup: this.groupName,
      queueId: messageQueue.queueId,
    };
    const findBrokerResult = await this.findBrokerAddressInAdmin(messageQueue);
    return await this.mqClient.queryConsumerOffset(findBrokerResult.brokerAddr, requestHeader, 1000 * 5);
  }

  /**
   * update consume offset to broker
   * @param {MessageQueue} messageQueue - message queue
   * @param {Number} offset - offset
   * @return {void}
   */
  async updateConsumeOffsetToBroker(messageQueue, offset) {
    const requestHeader = {
      topic: messageQueue.topic,
      consumerGroup: this.groupName,
      queueId: messageQueue.queueId,
      commitOffset: offset,
    };
    const findBrokerResult = await this.findBrokerAddressInAdmin(messageQueue);
    // use oneway, cause this action may cost time.
    await this.mqClient.updateConsumerOffsetOneway(findBrokerResult.brokerAddr, requestHeader);
  }

  async findBrokerAddressInAdmin(messageQueue) {
    let findBrokerResult = this.mqClient.findBrokerAddressInAdmin(messageQueue.brokerName);
    if (!findBrokerResult) {
      await this.mqClient.updateTopicRouteInfoFromNameServer(messageQueue.topic);
      findBrokerResult = this.mqClient.findBrokerAddressInAdmin(messageQueue.brokerName);
      if (!findBrokerResult) {
        throw new Error(`The broker[${messageQueue.brokerName}] not exist`);
      }
    }
    return findBrokerResult;
  }
}

module.exports = RemoteBrokerOffsetStore;
