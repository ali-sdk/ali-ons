'use strict';

const mm = require('mm');
const path = require('path');
const osenv = require('osenv');
const assert = require('assert');
const httpclient = require('urllib');
const Message = require('../').Message;
const Consumer = require('../').Consumer;
const Producer = require('../').Producer;
const sleep = require('mz-modules/sleep');
const rimraf = require('mz-modules/rimraf');
const config = require('../example/config');
const TopicPublishStatically = require('../example/topic_publish_statically');
const AllocateMessageQueueStatically = require('../example/allocate_message_queue_statically');

const TOPIC = config.topic;
const localOffsetStoreDir = path.join(osenv.home(), '.rocketmq_offsets_node');

describe('test/customize.test.js', () => {
  let consumer;
  let producer;

  before(() => {
    return rimraf(localOffsetStoreDir);
  });
  before(async () => {
    consumer = new Consumer(Object.assign({
      httpclient,
      persistent: true,
      rebalanceInterval: 2000,
      isBroadcast: true,
      allocateQueueManually: true,
      allocateMessageQueueStrategy: new AllocateMessageQueueStatically(),
      topicPublishInfo: TopicPublishStatically,
    }, config));
    producer = new Producer(Object.assign({
      httpclient,
      topicPublishInfo: TopicPublishStatically,
    }, config));

    await consumer.ready();
    await producer.ready();
  });

  after(async () => {
    await producer.close();
    const originFn = consumer._offsetStore.persistAll;
    mm(consumer._offsetStore, 'persistAll', async mqs => {
      assert(mqs && mqs.length);
      return await originFn.call(consumer._offsetStore, mqs);
    });
    await consumer.close();
  });

  afterEach(mm.restore);

  it('should subscribe message ok', async () => {
    const sent = new Set();
    const received = new Set();
    consumer.subscribe(TOPIC, 'TagA', async msg => {
      console.log('message receive ------------> ', msg.msgId, msg.body.toString());
      assert(msg.tags !== 'TagB');
      received.add(msg.msgId);
    });

    await sleep(5000);

    for (let i = 0; i < 10; i++) {
      const msg = new Message(TOPIC, // topic
        'TagA', // tag
        'Hello MetaQ !!! ' // body
      );
      const sendResult = await producer.send(msg);
      assert(sendResult && sendResult.msgId);
      console.log('send message success,', sendResult.msgId, sendResult.messageQueue.key, sendResult.queueOffset);
      sent.add(sendResult.msgId);
    }

    await sleep(5000);

    for (const msgId of sent.values()) {
      assert(received.has(msgId));
    }
  });
});
