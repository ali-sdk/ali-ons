'use strict';

const assert = require('assert');
const httpclient = require('urllib');
const MQClient = require('../../lib/mq_client');
const ClientConfig = require('../../lib/client_config');
const MessageQueue = require('../../lib/message_queue');
const LocalFileOffsetStore = require('../../lib/store/local_file');

describe('test/store/local_file.test.js', function() {

  before(async () => {
    const client = new MQClient(new ClientConfig({
      instanceName: Date.now() + '',
      httpclient,
    }));
    await client.ready();
    this.store = new LocalFileOffsetStore(client, 'please_rename_unique_group_name_1');
  });

  it('should load ok', () => {
    return this.store.load();
  });

  it('should updateOffset ok', () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000, true);
    assert(this.store.offsetTable.get('[topic="TopicTest_1", brokerName="taobaodaily-04", queueId="1"]') === 1000);
    this.store.updateOffset(null, 1000);
  });

  it('should readOffset ok', async () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000, true);
    let offset = await this.store.readOffset(mq, 'READ_FROM_MEMORY');
    assert(offset === 1000);

    offset = await this.store.readOffset(mq, 'READ_FROM_STORE');
    assert(typeof offset === 'number');

    offset = await this.store.readOffset(null, 'READ_FROM_STORE');
    assert(offset === -1);

    const mq1 = new MessageQueue('TopicTest_1', 'xxx', 1);
    offset = await this.store.readOffset(mq1, 'READ_FROM_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);

    const mq2 = new MessageQueue('TopicTest_1', 'yyy', 1);
    offset = await this.store.readOffset(mq2, 'MEMORY_FIRST_THEN_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);
  });

  it('should persistAll ok', async () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000);
    await this.store.persistAll([ mq ]);
    await this.store.persistAll([]);
    await this.store.persistAll(null);
  });

  it('should persist ok', async () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000);
    await this.store.persist(mq);
    const mq1 = new MessageQueue('TopicTest_1', 'zzz', 1);
    await this.store.persist(mq1);
  });

  it('should removeOffset ok', () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000);
    this.store.removeOffset(mq);
    assert(!this.store.offsetTable.has(mq.key));
  });
});
