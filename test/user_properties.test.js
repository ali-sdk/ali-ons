'use strict';

const mm = require('mm');
const assert = require('assert');
const httpclient = require('urllib');
const config = require('../example/config');
const Consumer = require('../').Consumer;
const Producer = require('../').Producer;
const sleep = require('mz-modules/sleep');
const Message = require('../').Message;


const TOPIC = config.topic;

describe('test/user_properties.test.js', () => {
  let producer,
    consumer;

  before(async () => {
    producer = new Producer(Object.assign({
      httpclient,
    }, config));
    await producer.ready();
    consumer = new Consumer(Object.assign({
      httpclient,
    }, config));
    await consumer.ready();
  });

  after(async () => {
    await producer.close();
    await consumer.close();
  });

  afterEach(mm.restore);


  const tests = [
    { tags: null, body: 'Test body', userProperties: { prop1: 'val1', prop2: 'val2' } },
  ];

  tests.forEach(test => {
    it('should send and receive messages with user-defined properties', async () => {
      await sleep(3000);

      const tagsString = test.tags ? test.tags.join('||') : '*';
      const msg = new Message(
        TOPIC,
        tagsString,
        test.body
      );

      test.userProperties && msg.setUserProperties(test.userProperties);

      const sendResult = await producer.send(msg);
      assert(sendResult && sendResult.msgId);

      const msgId = sendResult.msgId;
      console.log(`Send Result: ${JSON.stringify(sendResult, null, 2)}`);

      await new Promise(r => {
        consumer.subscribe(TOPIC, tagsString, async msg => {
          if (msg.msgId === msgId) {
            test.userProperties && Object.entries(test.userProperties)
              .forEach(([ k, v ]) => {
                assert(msg.properties[k] === v);
              });
            test.body && assert(msg.body.toString() === test.body);
            r();
          }
        });
      });
    }).timeout(20000);

  });
});
