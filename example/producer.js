'use strict';

const logger = require('./logger');
const config = require('./config');
const httpclient = require('urllib');
const Producer = require('../').Producer;
const Message = require('../').Message;

const TopicPublishStatically = require('./topic_publish_statically');

const producer = new Producer(Object.assign({
  httpclient,
  logger,
  topicPublishInfo: TopicPublishStatically,
}, config));
(async () => {
  await producer.ready();
  for (let i = 0; i < 100; i++) {
    try {
      const msg = new Message(config.topic, // topic
        'TagA', // tag
        'Hello ONS !!! ' // body
      );

      const sendResult = await producer.send(msg);
      console.log(sendResult.msgId, sendResult.messageQueue.key);
    } catch (err) {
      console.error(err)
    }
  }
})();
