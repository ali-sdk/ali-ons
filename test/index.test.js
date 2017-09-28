'use strict';

const mm = require('mm');
const co = require('co');
const path = require('path');
const osenv = require('osenv');
const assert = require('assert');
const httpclient = require('urllib');
const utils = require('../lib/utils');
const Message = require('../').Message;
const Consumer = require('../').Consumer;
const Producer = require('../').Producer;
const sleep = require('mz-modules/sleep');
const rimraf = require('mz-modules/rimraf');
const config = require('../example/config');

const localOffsetStoreDir = path.join(osenv.home(), '.rocketmq_offsets_node');

describe('test/index.test.js', () => {
  describe('API', () => {
    let producer;
    let consumer;
    before(function* () {
      producer = new Producer(Object.assign({
        httpclient,
        retryAnotherBrokerWhenNotStoreOK: true,
        compressMsgBodyOverHowmuch: 10,
      }, config));
      yield producer.ready();
      consumer = new Consumer(Object.assign({
        httpclient,
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

      mm(producer._mqClient, 'findBrokerAddressInPublish', () => {
        mm.restore();
        return null;
      });
      sendResult = yield producer.send(msg);
      assert(sendResult && sendResult.msgId);

      mm(producer._mqClient, 'findBrokerAddressInPublish', () => {
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

    it.skip('should updateProcessQueueTableInRebalance ok', function* () {
      consumer.subscribe('ALIPUSH_DISPATCH', '*', function* (msg) {
        console.log('message receive ------------> ', msg.body.toString());
      });
      yield sleep(5000);
      yield consumer.updateProcessQueueTableInRebalance('xxx', []);
      assert(consumer.processQueueTable.size > 0);

      yield consumer.updateProcessQueueTableInRebalance('ALIPUSH_DISPATCH', []);
      assert(consumer.processQueueTable.size > 0);
    });
  });

  // 广播消费
  describe('broadcast', () => {
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
        before(() => {
          return rimraf(localOffsetStoreDir);
        });
        before(function* () {
          consumer = new Consumer(Object.assign({
            httpclient,
            consumeFromWhere,
            isBroadcast: true,
            rebalanceInterval: 2000,
          }, config));
          producer = new Producer(Object.assign({
            httpclient,
          }, config));
          yield [
            consumer.ready(),
            producer.ready(),
          ];
        });

        after(function* () {
          yield producer.close();
          yield consumer.close();
        });

        afterEach(mm.restore);

        it('should subscribe message ok', function* () {
          let msgId;
          consumer.subscribe('TEST_TOPIC', 'TagA', function* (msg) {
            console.log('message receive ------------> ', msg.body.toString());
            assert(msg.tags !== 'TagB');
            if (msg.msgId === msgId) {
              assert(msg.body.toString() === 'Hello MetaQ !!! ');
              consumer.emit('TagA');
            }
          });

          yield sleep(5000);

          let msg = new Message('TEST_TOPIC', // topic
            'TagB', // tag
            'Hello MetaQ !!! ' // body
          );
          let sendResult = yield producer.send(msg);
          assert(sendResult && sendResult.msgId);
          msg = new Message('TEST_TOPIC', // topic
            'TagA', // tag
            'Hello MetaQ !!! ' // body
          );
          sendResult = yield producer.send(msg);
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
  describe('cluster', () => {
    const topic = 'TEST_TOPIC';
    let consumer;
    let producer;
    before(function* () {
      consumer = new Consumer(Object.assign({
        httpclient,
        isBroadcast: false,
      }, config));
      yield consumer.ready();
      producer = new Producer(Object.assign({
        httpclient,
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
        consumer.subscribe(topic, '*', function* (msg) {
          if (msg.msgId === msgId) {
            assert(msg.body.toString() === 'Hello MetaQ !!! ');
            cb();
          }
        });
      };
    });
  });

  describe('process exception', () => {
    let consumer;
    let producer;
    before(function* () {
      consumer = new Consumer(Object.assign({
        httpclient,
        rebalanceInterval: 2000,
      }, config));
      producer = new Producer(Object.assign({
        httpclient,
      }, config));
      yield [
        consumer.ready(),
        producer.ready(),
      ];
    });

    after(function* () {
      yield producer.close();
      yield consumer.close();
    });

    it('should retry if process failed', function* () {
      let msgId;
      consumer.subscribe('TEST_TOPIC', '*', function* (msg) {
        console.log('message receive ------------> ', msg.body.toString());
        if (msg.msgId === msgId) {
          assert(msg.body.toString() === 'Hello MetaQ !!! ');
          if (msg.reconsumeTimes === 0) {
            throw new Error('mock error');
          }
          consumer.emit('*');
        }
      });

      yield sleep(5000);

      const msg = new Message('TEST_TOPIC', // topic
        'TagA', // tag
        'Hello MetaQ !!! ' // body
      );
      const sendResult = yield producer.send(msg);
      assert(sendResult && sendResult.msgId);
      msgId = sendResult.msgId;
      yield consumer.await('*');
    });
  });

  describe('flow control', () => {
    let consumer;
    let producer;
    before(function* () {
      yield rimraf(localOffsetStoreDir);
      consumer = new Consumer(Object.assign({
        httpclient,
        isBroadcast: true,
        rebalanceInterval: 2000,
        pullThresholdForQueue: 1,
        consumeFromWhere: 'CONSUME_FROM_FIRST_OFFSET',
        pullTimeDelayMillsWhenFlowControl: 5000,
      }, config));
      producer = new Producer(Object.assign({
        httpclient,
      }, config));
      yield [
        consumer.ready(),
        producer.ready(),
      ];
    });

    after(function* () {
      yield producer.close();
      yield consumer.close();
    });

    it('should retry if process failed', function* () {
      let count = 20;
      while (count--) {
        const msg = new Message('TEST_TOPIC', // topic
          'TagA', // tag
          'Hello MetaQ !!! ' // body
        );
        const sendResult = yield producer.send(msg);
        assert(sendResult && sendResult.msgId);
      }

      consumer.subscribe('TEST_TOPIC', '*', function* (msg, mq, pq) {
        console.log('message receive ------------> ', msg.body.toString());

        const msgCount = pq.msgCount;
        yield sleep(10000);

        // 不再拉取
        try {
          console.info('----------------> origin msgCount %d, current msgCount %d', msgCount, pq.msgCount);
          assert(msgCount === 1 || pq.msgCount === msgCount);
          consumer.emit('over');
        } catch (err) {
          consumer.emit('error', err);
        }
      });

      yield Promise.race([
        consumer.await('over'),
        consumer.await('error'),
      ]);
    });
  });
});
