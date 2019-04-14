'use strict';

const assert = require('assert');
const httpclient = require('urllib');
const MQClient = require('../../lib/mq_client');
const config = require('../../example/config');
const ClientConfig = require('../../lib/client_config');
const MessageQueue = require('../../lib/message_queue');
const RemoteBrokerOffsetStore = require('../../lib/store/remote_broker');

const TOPIC = config.topic;
const consumerGroup = config.consumerGroup;

describe('test/store/remote_broker.test.js', function() {
  let client;
  let brokerName;
  before(async () => {
    client = new MQClient(new ClientConfig(Object.assign({ httpclient }, config)));
    await client.ready();
    const routerInfoData = await client.getDefaultTopicRouteInfoFromNameServer(TOPIC, 3000);
    brokerName = routerInfoData.brokerDatas[0].brokerName;
    this.store = new RemoteBrokerOffsetStore(client, consumerGroup);
  });

  after(() => client.close());

  it('should load ok', () => this.store.load());

  it('should updateOffset ok', () => {
    const mq = new MessageQueue(TOPIC, brokerName, 1);
    this.store.updateOffset(mq, 1000);
    assert(this.store.offsetTable.get(`[topic="${TOPIC}", brokerName="${brokerName}", queueId="1"]`) === 1000);
    this.store.updateOffset(null, 1000);
  });

  it('should readOffset ok', async () => {
    const mq = new MessageQueue(TOPIC, brokerName, 1);
    this.store.updateOffset(mq, 1000);
    let offset = await this.store.readOffset(mq, 'READ_FROM_MEMORY');
    assert(offset === 1000);

    offset = await this.store.readOffset(mq, 'READ_FROM_STORE');
    assert(typeof offset === 'number');

    offset = await this.store.readOffset(null, 'READ_FROM_STORE');
    assert(offset === -1);

    const mq1 = new MessageQueue(TOPIC, 'xxx', 1);
    offset = await this.store.readOffset(mq1, 'READ_FROM_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);

    const mq2 = new MessageQueue(TOPIC, 'yyy', 1);
    offset = await this.store.readOffset(mq2, 'MEMORY_FIRST_THEN_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);
  });

  it('should persistAll ok', async () => {
    const mq = new MessageQueue(TOPIC, brokerName, 1);
    this.store.updateOffset(mq, 1000);
    await this.store.persistAll([ mq ]);
    await this.store.persistAll([]);
    await this.store.persistAll(null);
  });

  it('should persist ok', async () => {
    const mq = new MessageQueue(TOPIC, brokerName, 1);
    this.store.updateOffset(mq, 1000);
    await this.store.persist(mq);
    const mq1 = new MessageQueue(TOPIC, 'zzz', 1);
    await this.store.persist(mq1);
  });

  it('should removeOffset ok', () => {
    const mq = new MessageQueue(TOPIC, brokerName, 1);
    this.store.updateOffset(mq, 1000);
    this.store.removeOffset(mq);
    assert(!this.store.offsetTable.has(mq.key));
  });
});
