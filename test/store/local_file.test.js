'use strict';

const assert = require('assert');
const httpclient = require('urllib');
const MQClient = require('../../lib/mq_client');
const ClientConfig = require('../../lib/client_config');
const MessageQueue = require('../../lib/message_queue');
const LocalFileOffsetStore = require('../../lib/store/local_file');

describe('test/store/local_file.test.js', function() {

  before(function* () {
    const client = new MQClient(new ClientConfig({
      instanceName: Date.now() + '',
      httpclient,
    }));
    yield client.ready();
    this.store = new LocalFileOffsetStore(client, 'please_rename_unique_group_name_1');
  });

  it('should load ok', function* () {
    yield this.store.load();
  });

  it('should updateOffset ok', function() {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000, true);
    assert(this.store.offsetTable.get('[topic="TopicTest_1", brokerName="taobaodaily-04", queueId="1"]') === 1000);
    this.store.updateOffset(null, 1000);
  });

  it('should readOffset ok', function* () {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000, true);
    let offset = yield this.store.readOffset(mq, 'READ_FROM_MEMORY');
    assert(offset === 1000);

    offset = yield this.store.readOffset(mq, 'READ_FROM_STORE');
    assert(typeof offset === 'number');

    offset = yield this.store.readOffset(null, 'READ_FROM_STORE');
    assert(offset === -1);

    const mq1 = new MessageQueue('TopicTest_1', 'xxx', 1);
    offset = yield this.store.readOffset(mq1, 'READ_FROM_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);

    const mq2 = new MessageQueue('TopicTest_1', 'yyy', 1);
    offset = yield this.store.readOffset(mq2, 'MEMORY_FIRST_THEN_STORE');
    assert(typeof offset === 'number');
    assert(offset === -1);
  });

  it('should persistAll ok', function* () {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000);
    yield this.store.persistAll([ mq ]);
    yield this.store.persistAll([]);
    yield this.store.persistAll(null);
  });

  it('should persist ok', function* () {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000);
    yield this.store.persist(mq);
    const mq1 = new MessageQueue('TopicTest_1', 'zzz', 1);
    yield this.store.persist(mq1);
  });

  it('should removeOffset ok', function() {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    this.store.updateOffset(mq, 1000);
    this.store.removeOffset(mq);
    assert(!this.store.offsetTable.has(mq.key));
  });
});
