'use strict';

const mm = require('mm');
const assert = require('assert');
const MessageQueue = require('../../lib/message_queue');
const ReadOffsetType = require('../../lib/store/read_offset_type');
const LocalMemoryOffsetStore = require('../../lib/store/local_memory');

describe('test/store/local_file.test.js', () => {
  let offsetStore;
  const groupName = 'S_appname_service';
  before(function* () {
    offsetStore = new LocalMemoryOffsetStore({
      logger: console,
    }, groupName);
  });
  beforeEach(mm.restore);

  it('should updateOffset & readOffset ok', function* () {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    offsetStore.updateOffset(mq, 1000, false);
    let offset = yield offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    assert(offset === 1000);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 1000);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === 1000);

    yield offsetStore.persist(mq);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === 1000);

    const mq2 = new MessageQueue('TopicTest_1', 'taobaodaily-04', 2);
    yield offsetStore.persistAll([ mq, mq2 ]);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === 1000);
  });

  it('should updateOffset increaseOnly', function* () {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    offsetStore.updateOffset(mq, 1000, false);
    let offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 1000);

    offsetStore.updateOffset();
    offsetStore.updateOffset(mq, 999, true);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 1000);

    offsetStore.updateOffset(mq, 999, false);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === 999);

    offset = yield offsetStore.readOffset(null, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);
    offset = yield offsetStore.readOffset(null, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    assert(offset === -1);
    offset = yield offsetStore.readOffset(null, ReadOffsetType.READ_FROM_STORE);
    assert(offset === -1);
    offset = yield offsetStore.readOffset(mq);
    assert(offset === -1);
  });

  it('should load ok', function* () {
    const mq = new MessageQueue('TopicTest_1', 'taobaodaily-04', 1);
    offsetStore.updateOffset(mq, 1000, false);

    const offsetStore2 = new LocalMemoryOffsetStore({
      logger: console,
    }, groupName);
    let offset = yield offsetStore2.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);
    yield offsetStore2.load();
    offset = yield offsetStore2.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);

    offsetStore.removeOffset(mq);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_MEMORY);
    assert(offset === -1);
    offset = yield offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
    assert(offset === -1);
  });
});
