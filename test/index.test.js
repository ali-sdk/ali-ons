'use strict';

const mm = require('mm');
const co = require('co');
const path = require('path');
const osenv = require('osenv');
const assert = require('assert');
const pedding = require('pedding');
const rimraf = require('rimraf');
const urllib = require('urllib');
const utils = require('../lib/utils');
const Message = require('../').Message;
const Consumer = require('../').Consumer;
const Producer = require('../').Producer;
const config = require('../example/config');

const localOffsetStoreDir = path.join(osenv.home(), '.rocketmq_offsets_node');
const sleep = timeout => callback => setTimeout(callback, timeout);

describe('test/index.test.js', function() {

  describe('API', function* () {
    let producer;
    let consumer;
    before(function* () {
      producer = new Producer(Object.assign({
        urllib,
        retryAnotherBrokerWhenNotStoreOK: true,
        compressMsgBodyOverHowmuch: 10,
      }, config));
      yield producer.ready();
      consumer = new Consumer(Object.assign({
        urllib,
        rebalanceInterval: 1000,
        isBroadcast: true,
      }, config));
      yield consumer.ready();
    });

    after(function* () {
      yield producer.close();
      yield consumer.close();
    });

    afterEach(mm.restore);

    // it.skip('it should create topic ok', function*() {
    //   yield producer.createTopic('TBW102', 'XXX', 8);
    // });

    it('should select another broker if one failed', function* () {
      mm(producer, 'sendKernelImpl', function* () {
        mm.restore();
        return {
          sendStatus: 'FLUSH_DISK_TIMEOUT',
        };
      });
      const msg = new Message('TEST_TOPIC', // topic
        'TagA', // tag
        'Hello TagA !!! ' // body
      );

      let sendResult = yield producer.send(msg);
      assert(sendResult && sendResult.msgId);

      mm(producer, 'sendKernelImpl', function* () {
        mm.restore();
        const err = new Error('mock err');
        err.name = 'MQClientException';
        err.code = 205;
        throw err;
      });
      sendResult = yield producer.send(msg);
      assert(sendResult && sendResult.msgId);
    });

    it('should tryToFindTopicPublishInfo if brokerAddr not found', function* () {
      const msg = new Message('TEST_TOPIC', // topic
        'TagA', // tag
        'Hello TagA !!! ' // body
      );

      let sendResult = yield producer.send(msg);
      assert(sendResult && sendResult.msgId);

      mm(producer.mqClient, 'findBrokerAddressInPublish', () => {
        mm.restore();
        return null;
      });
      sendResult = yield producer.send(msg);
      assert(sendResult && sendResult.msgId);

      mm(producer.mqClient, 'findBrokerAddressInPublish', () => {
        return null;
      });

      let isError = false;
      try {
        yield producer.send(msg);
      } catch (err) {
        isError = true;
        assert(/The broker\[.+\] not exist/i.test(err.message));
        assert(err.name === 'MQClientException');
      }
      assert(isError);
      mm.restore();

      isError = false;
      try {
        yield producer.send(new Message('%RETRY%TEST_TOPIC', // topic
          'TagA', // tag
          'Hello TagA !!! ' // body
        ));
      } catch (err) {
        isError = true;
        assert(err.name === 'MQClientException');
      }
      assert(isError);
    });

    it('should emit error if compress failed', done => {
      mm(utils, 'compress', function() {
        throw new Error('mock error');
      });

      const msg = new Message('TEST_TOPIC', // topic
        'TagA', // tag
        'Hello TagA !!!   sds' // body
      );
      producer.on('error', err => {
        assert(err.name === 'MetaQCompressError');
        done();
      });

      co(function* () {
        const sendResult = yield producer.send(msg);
        assert(sendResult && sendResult.msgId);
      }).then(() => console.log('over')).catch(err => console.log(err));
    });

    it('should updateProcessQueueTableInRebalance ok', function* () {
      consumer.subscribe('ALIPUSH_DISPATCH', '*');
      consumer.on('message', msgs => {
        for (const msg of msgs) {
          console.log('message receive ------------> ', msg.body.toString());
        }
      });
      yield sleep(5000);
      yield consumer.updateProcessQueueTableInRebalance('xxx', []);
      assert(consumer.processQueueTable.size > 0);

      yield consumer.updateProcessQueueTableInRebalance('ALIPUSH_DISPATCH', []);
      assert(consumer.processQueueTable.size > 0);
    });
  });

  // 广播消费
  describe('broadcast', function() {

    [
      'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST',
      'CONSUME_FROM_MIN_OFFSET',
      'CONSUME_FROM_MAX_OFFSET',
      'CONSUME_FROM_LAST_OFFSET',
      'CONSUME_FROM_FIRST_OFFSET',
      'CONSUME_FROM_TIMESTAMP',
    ].forEach(consumeFromWhere => {
      let consumer;
      let producer;
      describe(`consumeFromWhere => ${consumeFromWhere}`, () => {
        before(function(done) {
          if (consumeFromWhere === 'CONSUME_FROM_TIMESTAMP') {
            rimraf.sync(localOffsetStoreDir);
          }

          done = pedding(done, 2);
          consumer = new Consumer(Object.assign({
            urllib,
            consumeFromWhere,
            isBroadcast: true,
            rebalanceInterval: 2000,
          }, config));
          consumer.ready(done);
          producer = new Producer(Object.assign({
            urllib,
          }, config));
          producer.ready(done);
        });

        after(function* () {
          yield producer.close();
          yield consumer.close();
        });

        afterEach(mm.restore);

        it('should subscribe message ok', function* () {
          const msg = new Message('TEST_TOPIC', // topic
            'TagA', // tag
            'Hello MetaQ !!! ' // body
          );
          consumer.subscribe('TEST_TOPIC', '*');
          let msgId;
          consumer.once('message', msgs => {
            for (const msg of msgs) {
              console.log('message receive ------------> ', msg.body.toString());
            }
          });
          consumer.on('message', msgs => {
            msgs.forEach(msg => {
              console.log('message receive ------------> ', msg.body.toString());
              if (msg.msgId === msgId) {
                assert(msg.body.toString() === 'Hello MetaQ !!! ');
                consumer.emit('*');
              }
            });
          });

          yield sleep(5000);

          const sendResult = yield producer.send(msg);
          assert(sendResult && sendResult.msgId);
          msgId = sendResult.msgId;

          yield consumer.await('*');
        });

        it('should subscribe message with tags ok', function* () {
          const msg = new Message('TEST_TOPIC', // topic
            'TagA', // tag
            'Hello TagA !!! ' // body
          );
          consumer.subscribe('TEST_TOPIC', 'TagA');
          let msgId;
          consumer.once('message', msgs => {
            for (const msg of msgs) {
              console.log('message receive ------------> ', msg.body.toString());
            }
          });
          consumer.on('message', msgs => {
            msgs.forEach(msg => {
              console.log('message receive ------------> ', msg.body.toString());
              if (msg.msgId === msgId) {
                assert(msg.body.toString() === 'Hello TagA !!! ');
                consumer.emit('TagA');
              }
            });
          });

          yield sleep(5000);

          const sendResult = yield producer.send(msg);
          assert(sendResult && sendResult.msgId);
          msgId = sendResult.msgId;

          yield consumer.await('TagA');
        });

        it.skip('should viewMessage ok', function* () {
          const msg = new Message('TEST_TOPIC', // topic
            'TagA', // tag
            'Hello MetaQ !!! ' // body
          );
          const sendResult = yield producer.send(msg);
          assert(sendResult && sendResult.msgId);

          yield sleep(3000);

          const message = yield consumer.viewMessage(sendResult.msgId);
          assert(message.body.toString() === 'Hello MetaQ !!! ');
        });
      });
    });
  });

  // 集群消费
  describe('cluster', function() {
    const topic = 'TEST_TOPIC';
    let consumer;
    let producer;
    before(function* () {
      consumer = new Consumer(Object.assign({
        urllib,
        isBroadcast: false,
      }, config));
      yield consumer.ready();
      producer = new Producer(Object.assign({
        urllib,
      }, config));
      yield producer.ready();
    });

    after(function* () {
      yield consumer.close();
      yield producer.close();
    });

    afterEach(mm.restore);

    it('should subscribe message ok', function* () {
      yield sleep(5000);

      const msg = new Message(topic, // topic
        'TagA', // tag
        'Hello MetaQ !!! ' // body
      );
      const sendResult = yield producer.send(msg);
      assert(sendResult && sendResult.msgId);

      const msgId = sendResult.msgId;
      console.log(sendResult);

      yield cb => {
        function onMessage(msgs, complete) {
          msgs.forEach(function(msg) {
            if (msg.msgId === msgId) {
              assert(msg.body.toString() === 'Hello MetaQ !!! ');
              consumer.removeListener('message', onMessage);
              cb();
            }
          });
          complete();
        }
        consumer.on('message', onMessage);
        consumer.subscribe(topic, '*');
      };
    });
  });
});
