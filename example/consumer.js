'use strict';

const urllib = require('urllib');
const Consumer = require('../').Consumer;
const config = require('./config');
const consumer = new Consumer(Object.assign(config, {
  urllib,
  // isBroadcast: true,
  consumeFromWhere: 'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST',
}));

consumer.subscribe(config.topic, '*');

consumer.on('message', (msgs, done) => {
  msgs.forEach(msg => console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`));
  done();
});

consumer.on('error', err => console.log(err));
