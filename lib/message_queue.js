'use strict';

const fmt = require('util').format;

class MessageQueue {
  constructor(topic, brokerName, queueId) {
    this.topic = topic;
    this.brokerName = brokerName;
    this.queueId = queueId;

    this.key = fmt('[topic="%s", brokerName="%s", queueId="%s"]', this.topic, this.brokerName, this.queueId);
  }
}

module.exports = MessageQueue;
