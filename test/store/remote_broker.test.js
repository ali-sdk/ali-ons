'use strict';

const assert = require('assert');
const httpclient = require('urllib');
const MQClient = require('../../lib/mq_client');
const config = require('../../example/config');
const ClientConfig = require('../../lib/client_config');
const MessageQueue = require('../../lib/message_queue');
const RemoteBrokerOffsetStore = require('../../lib/store/remote_broker');

describe('test/store/remote_broker.test.js', function() {
  let client;
  let brokerName;
  before(function* () {
    client = new MQClient(new ClientConfig(Object.assign({ httpclient }, config)));
    yield client.ready();
    const routerInfoData = yield client.getDefaultTopicRouteInfoFromNameServer('TEST_TOPIC', 3000);
    brokerName = routerInfoData.brokerDatas[0].brokerName;
    this.store = new RemoteBrokerOffsetStore(client, 'CID_47716977-101');
  });

  after(function* () {
    yield client.close();
  });

  it('should load ok', function* () {
    yield this.store.load();
  });

  it('should updateOffset ok', function() {
    const mq = new MessageQueue('TEST_TOPIC', brokerName, 1);
    this.store.updateOffset(mq, 1000);
    assert(this.store.offsetTable.get(`[topic="TEST_TOPIC", brokerName="${brokerName}", queueId="1"]`) === 1000);
    this.store.updateOffset(null, 1000);
  });

  it('should readOffset ok', function* () {
    const mq = new MessageQueue('TEST_TOPIC', brokerName, 1);
    this.store.updateOffset(mq, 1000);
    let offset = yield this.store.readOffset(mq, 'READ_FROM_MEMORY');
    assert(offset === 1000);

    offset = yield this.store.readOffset(mq, 'READ_FROM_STORE');
    assert(typeof offset === 'number');

    offset = yield this.store.readOffset(null, 'READ_FROM_STORE');
    assert(offset === -1);

    const mq1 = new MessageQueue('TEST_TOPIC', 'xxx', 1);
    offset = yield this.store.readOffset(mq1, 'READ_FROM_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);

    const mq2 = new MessageQueue('TEST_TOPIC', 'yyy', 1);
    offset = yield this.store.readOffset(mq2, 'MEMORY_FIRST_THEN_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);
  });

  it('should persistAll ok', function* () {
    const mq = new MessageQueue('TEST_TOPIC', brokerName, 1);
    this.store.updateOffset(mq, 1000);
    yield this.store.persistAll([ mq ]);
    yield this.store.persistAll([]);
    yield this.store.persistAll(null);
  });

  it('should persist ok', function* () {
    const mq = new MessageQueue('TEST_TOPIC', brokerName, 1);
    this.store.updateOffset(mq, 1000);
    yield this.store.persist(mq);
    const mq1 = new MessageQueue('TEST_TOPIC', 'zzz', 1);
    yield this.store.persist(mq1);
  });

  it('should removeOffset ok', function() {
    const mq = new MessageQueue('TEST_TOPIC', brokerName, 1);
    this.store.updateOffset(mq, 1000);
    this.store.removeOffset(mq);
    assert(!this.store.offsetTable.has(mq.key));
  });
});
