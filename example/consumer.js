'use strict';

const httpclient = require('urllib');
const logger = require('./logger');
const Consumer = require('../').Consumer;
const config = require('./config');
const consumer = new Consumer(Object.assign(config, {
  httpclient,
  logger,
  // isBroadcast: true,
  consumeFromWhere: 'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST',
}));

consumer.subscribe(config.topic, '*', async function(msg) {
  console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`)
});

consumer.on('error', err => console.log(err));
