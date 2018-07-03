'use strict';

const logger = require('./logger');
const config = require('./config');
const httpclient = require('urllib');
const Producer = require('../').Producer;
const Message = require('../').Message;

const producer = new Producer(Object.assign({ httpclient, logger }, config));
(async () => {
  try {
    await producer.ready();
    const msg = new Message(config.topic, // topic
      'TagA', // tag
      'Hello ONS !!! ' // body
    );

    const sendResult = await producer.send(msg);
    console.log(sendResult);
  } catch (err) {
    console.error(err)
  }
})();
