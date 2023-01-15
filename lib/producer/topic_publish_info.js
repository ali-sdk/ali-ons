'use strict';

const crypto = require('crypto')

class TopicPublishInfo {
  constructor() {
    this.orderTopic = false;
    this.haveTopicRouterInfo = false;
    this.messageQueueList = [];
    this.sendWhichQueue = Math.floor(Math.random() * Math.floor(Math.pow(2, 31))); // 打散 queue 选择起点
  }

  /**
   * 是否可用
   */
  get ok() {
    return this.messageQueueList && this.messageQueueList.length;
  }

  /**
   * 如果lastBrokerName不为null，则寻找与其不同的MessageQueue
   * @param {String} lastBrokerName - last broker name
   * @return {Object} queue
   */
  selectOneMessageQueue(lastBrokerName, message) {
    let index,
      pos,
      mq;

    const shardingKey = message && message.shardingKey;
    let queueId = '';
    if (shardingKey) {
      // 这里不应该是所有可用队列数字，应该是queue总数
      // 但这里的 messageQueueList 是所有可写队列， golang和java客户端均为在queue总对恶劣中寻找位置
      // 通过寻找所有可写queueId总数计算应该写入的queueId
      const queueIds = [];
      this.messageQueueList.forEach(queue => {
        if (!queueIds.includes(queue.queueId)) queueIds.push(queue.queueId);
      });

      // 参考golang和java的sdk，没有规范确定的shardingKey找queueId的实现标准
      // 若引入其他hash算法就显得比较笨重, 可以使用 nodejs 原生hash方法生成可计算的hash值
      // Reference: https://github.com/apache/rocketmq/blob/c11ed78eeb73c23da4cf9a36a6bad493a1279210/proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/producer/SendMessageActivity.java#L382
      // Reference: https://github.com/apache/rocketmq-client-go/blob/896a8a3453be4bcfcb5bfb5bb9139c4df6d4cdab/producer/selector.go#L112
      const queueIndex = crypto.createHash('md5').update(shardingKey).digest().readUInt32BE() % queueIds.length;
      queueId = queueIds[queueIndex] || ''
    }

    if (!Number.isSafeInteger(this.sendWhichQueue)) {
      this.sendWhichQueue = 0; // 超出安全范围，重置为 0
    }
    index = this.sendWhichQueue++;
    for (let i = 0, len = this.messageQueueList.length; i < len; i++) {
      pos = Math.abs(index++) % len;
      mq = this.messageQueueList[pos];
      if (lastBrokerName && mq.brokerName === lastBrokerName) {
        continue;
      }
      if (queueId && mq.queueId !== queueId) {
        continue;
      }
      return mq;
    }
    return null;
  }
}

module.exports = TopicPublishInfo;
