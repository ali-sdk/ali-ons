'use strict';

const mm = require('mm');
const assert = require('assert');
const MessageQueue = require('../../lib/message_queue');
const ReadOffsetType = require('../../lib/store/read_offset_type');
const LocalMemoryOffsetStore = require('../../lib/store/local_memory');

describe('test/store/local_file.test.js', () => {
  let offsetStore;
  const groupName = 'S_appname_service';
  before(() => {
    offsetStore = new LocalMemoryOffsetStore({
      logger: console,
    }, groupName);
  });
  beforeEach(mm.restore);

  it('should updateOffset & readOffset ok', async () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    offsetStore.updateOffset(mq, 1000, false);
    let offset = await offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    assert(offset === 1000);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 1000);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === 1000);

    await offsetStore.persist(mq);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === 1000);

    const mq2 = new MessageQueue('TopicTest_1', 'taobaodaily-04', 2);
    await offsetStore.persistAll([ mq, mq2 ]);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === 1000);
  });

  it('should updateOffset increaseOnly', async () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    offsetStore.updateOffset(mq, 1000, false);
    let offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 1000);

    offsetStore.updateOffset();
    offsetStore.updateOffset(mq, 999, true);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 1000);

    offsetStore.updateOffset(mq, 999, false);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 999);

    offset = await offsetStore.readOffset(null, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);
    offset = await offsetStore.readOffset(null, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    assert(offset === -1);
    offset = await offsetStore.readOffset(null, ReadOffsetType.READ_FROM_STORE);
    assert(offset === -1);
    offset = await offsetStore.readOffset(mq);
    assert(offset === -1);
  });

  it('should load ok', async () => {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    offsetStore.updateOffset(mq, 1000, false);

    const offsetStore2 = new LocalMemoryOffsetStore({
      logger: console,
    }, groupName);
    let offset = await offsetStore2.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);
    await offsetStore2.load();
    offset = await offsetStore2.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);

    offsetStore.removeOffset(mq);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);
    offset = await offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === -1);
  });
});
