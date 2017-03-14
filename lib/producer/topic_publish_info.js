'use strict';

class TopicPublishInfo {
  constructor() {
    this.orderTopic = false;
    this.haveTopicRouterInfo = false;
    this.messageQueueList = [];
    this.sendWhichQueue = 0;
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
  selectOneMessageQueue(lastBrokerName) {
    let index,
      pos,
      mq;
    if (lastBrokerName) {
      index = this.sendWhichQueue++;
      for (let i = 0, len = this.messageQueueList.length; i < len; i++) {
        pos = Math.abs(index++) % len;
        mq = this.messageQueueList[pos];
        if (mq.brokerName !== lastBrokerName) {
          return mq;
        }
      }
      return null;
    }
    index = this.sendWhichQueue++;
    pos = Math.abs(index) % this.messageQueueList.length;
    return this.messageQueueList[pos];
  }
}

module.exports = TopicPublishInfo;
