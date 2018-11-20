'use strict';

class TopicPublishStatically {
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
   * @return {Object} queue
   */
  selectOneMessageQueue() {
    return this.messageQueueList[this.sendWhichQueue];
  }
}

module.exports = TopicPublishStatically;
