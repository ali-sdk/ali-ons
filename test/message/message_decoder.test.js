'use strict';

const assert = require('assert');
const ByteBuffer = require('byte');
const utils = require('../utils');
const Message = require('../../lib/message/message');
const MessageDecoder = require('../../lib/message/message_decoder');

const NAME_VALUE_SEPARATOR = String.fromCharCode(1);
const PROPERTY_SEPARATOR = String.fromCharCode(2);
const SYSTEM_PROP_KEY_STARTDELIVERTIME = '__STARTDELIVERTIME';

describe('test/message/message_decoder.test.js', function() {
  it('should decode message ok', function() {
    const buf = ByteBuffer.wrap(utils.bytes('message.bin'));
    const message = MessageDecoder.decode(buf);
    assert(message);
    assert(message.msgId === '0ADA91A6000029CC0000006500E59F3E');
    assert.deepEqual(message.body, Buffer.from('{"room":"1","msg":"1"}'));
    assert(message.tags === 'TagA');
    assert(!message.keys);
    assert(message.delayTimeLevel === 0);
    assert(!message.waitStoreMsgOK);
    assert(!message.buyerId);
    assert(message.topic === 'TopicTest');
  });

  it('should decode compress message ok', function() {
    const buf = ByteBuffer.wrap(utils.bytes('message_compress.bin'));
    const message = MessageDecoder.decode(buf);
    assert(message);
    assert(message.msgId === '0ADA91A6000029CC000000661A586D98');
    assert.deepEqual(message.body, Buffer.from('Hello MetaQ !!!'));
    assert(message.tags, 'TagA');
    assert(!message.keys);
    assert(message.delayTimeLevel === 0);
    assert(!message.waitStoreMsgOK);
    assert(!message.buyerId);
    assert(message.topic === 'TopicTest');
  });

  it('should decode message and not read body ok', function() {
    const buf = ByteBuffer.wrap(utils.bytes('message.bin'));
    const message = MessageDecoder.decode(buf, false);
    assert(message);
    assert(message.msgId === '0ADA91A6000029CC0000006500E59F3E');
    assert(!message.body);
    assert(message.tags === 'TagA');
    assert(!message.keys);
    assert(message.delayTimeLevel === 0);
    assert(!message.waitStoreMsgOK);
    assert(!message.buyerId);
    assert(message.topic === 'TopicTest');
  });

  it('should batch decode message ok', function() {
    const buf = ByteBuffer.wrap(utils.bytes('batch_message.bin'));
    const messages = MessageDecoder.decodes(buf);
    assert(messages.length === 3);
    assert(messages[0].msgId === '0ADA91A6000029CC000000659560A2AA');
    assert.deepEqual(messages[0].body, Buffer.from('Hello MetaQ !!!'));
    assert(messages[1].msgId === '0ADA91A6000029CC000000659560D349');
    assert(messages[2].msgId === '0ADA91A6000029CC000000659560D578');
  });

  it('should decodeMessageId ok', function() {
    const data = MessageDecoder.decodeMessageId('0ADA91A6000029CC0000006504009B6F');
    assert(data.address === '10.218.145.166:10700');
    assert(data.offset.toString() === '433858845551');
  });

  it('should messageProperties2String ok', function() {
    assert(MessageDecoder.messageProperties2String({
      foo: 'bar',
      xxx: 'yyy',
      a: '中文',
    }) === `foo${NAME_VALUE_SEPARATOR}bar${PROPERTY_SEPARATOR}xxx${NAME_VALUE_SEPARATOR}yyy${PROPERTY_SEPARATOR}a${NAME_VALUE_SEPARATOR}中文${PROPERTY_SEPARATOR}`);
  });

  it('should create message ok', function() {
    const message = new Message('fake_topic', 'fake body');
    message.tags = 'TagB';
    assert(message.properties.TAGS === 'TagB');
    message.keys = [ 'xxx', 'yyy' ];
    assert(message.properties.KEYS === 'xxx yyy');
    message.delayTimeLevel = 1000;
    assert(message.delayTimeLevel === 1000);
    assert(message.properties.DELAY === '1000');
    message.waitStoreMsgOK = true;
    assert(message.properties.WAIT === 'true');
    message.buyerId = '123';
    assert(message.properties.BUYER_ID === '123');
  });

  it('should create deliver time message ok', function() {
    const deliverTime = Date.now() + 3000;
    const message = new Message('fake_topic', 'fake body');
    message.setStartDeliverTime(deliverTime);
    assert(message.getStartDeliverTime() === deliverTime);
    assert(message.properties[SYSTEM_PROP_KEY_STARTDELIVERTIME] === deliverTime);
  });
});
