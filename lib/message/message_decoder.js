'use strict';
/* eslint no-bitwise: 0 */

const debug = require('debug')('mq:decoder');
const Long = require('long');
const is = require('is-type-of');
const utils = require('../utils');
const ByteBuffer = require('byte');
const Message = require('./message');
const MessageSysFlag = require('../utils/message_sys_flag');

const MSG_ID_LENGTH = 8 + 8;
const NAME_VALUE_SEPARATOR = String.fromCharCode(1);
const PROPERTY_SEPARATOR = String.fromCharCode(2);

const byteBufferMsgId = ByteBuffer.allocate(MSG_ID_LENGTH);

// 将属性转换为字符串
exports.messageProperties2String = function(properties) {
  let str = '';
  if (properties) {
    for (const key in properties) {
      str += key + NAME_VALUE_SEPARATOR;
      str += properties[key] + PROPERTY_SEPARATOR;
    }
  }
  return str;
};

// 解析 messageId，从中提取出 address 和 offset 信息
exports.decodeMessageId = function(msgId) {
  // 地址
  const host = string2bytes(msgId.slice(0, 8));
  const port = string2bytes(msgId.slice(8, 16)).readInt32BE(0);

  // offset
  const data = string2bytes(msgId.slice(16, 32));
  const offset = new Long(
    data.readInt32BE(4), // low, high
    data.readInt32BE(0)
  );

  return {
    address: host[0] + '.' + host[1] + '.' + host[2] + '.' + host[3] + ':' + port,
    offset,
  };
};

/**
 * 解析一条消息
 * @param {ByteBuffer} byteBuffer -
 * @param {Boolean} readBody - 是否读取 body 部分
 * @param {Boolean} deCompressBody - 是否解压 body
 * @return {RemotingCommand} command
 */
const decode = exports.decode = function(byteBuffer, readBody, deCompressBody) {
  if (is.nullOrUndefined(readBody)) {
    readBody = true;
  }
  if (is.nullOrUndefined(deCompressBody)) {
    deCompressBody = true;
  }
  try {
    return innerDecode(byteBuffer, readBody, deCompressBody);
  } catch (err) {
    debug('Error occurred during innerDecode, err: %s', err.stack);
    // 指向末尾
    byteBuffer.position(byteBuffer.limit());
  }
  return null;
};

/**
 * 执行解码
 * @param {ByteBuffer} byteBuffer -
 * @param {Boolean} readBody - 是否读取 body 部分
 * @param {Boolean} deCompressBody - 是否解压 body
 * @return {RemotingCommand} command
 */
function innerDecode(byteBuffer, readBody, deCompressBody) {
  const msgExt = new Message();
  // 消息ID
  byteBufferMsgId.reset();
  // 1 TOTALSIZE
  msgExt.storeSize = byteBuffer.getInt();
  // 2 MAGICCODE
  byteBuffer.getInt();
  // 3 BODYCRC
  msgExt.bodyCRC = byteBuffer.getInt();
  // 4 QUEUEID
  msgExt.queueId = byteBuffer.getInt();
  // 5 FLAG
  msgExt.flag = byteBuffer.getInt();
  // 6 QUEUEOFFSET
  msgExt.queueOffset = byteBuffer.getLong().toNumber();
  // 7 PHYSICALOFFSET
  msgExt.commitLogOffset = byteBuffer.getLong().toNumber();
  // 8 SYSFLAG
  const sysFlag = byteBuffer.getInt();
  msgExt.sysFlag = sysFlag;
  // 9 BORNTIMESTAMP
  msgExt.bornTimestamp = byteBuffer.getLong().toNumber();
  // 10 BORNHOST
  const host = new Buffer(4);
  byteBuffer.get(host);
  let port = byteBuffer.getInt();
  msgExt.bornHost = host[0] + '.' + host[1] + '.' + host[2] + '.' + host[3] + ':' + port;
  // 11 STORETIMESTAMP
  msgExt.storeTimestamp = byteBuffer.getLong().toNumber();
  // 12 STOREHOST
  host.fill(0);
  byteBuffer.get(host);
  port = byteBuffer.getInt();
  msgExt.storeHost = host[0] + '.' + host[1] + '.' + host[2] + '.' + host[3] + ':' + port;
  byteBufferMsgId.put(host);
  byteBufferMsgId.putInt(port);
  byteBufferMsgId.putLong(msgExt.commitLogOffset);
  // 13 RECONSUMETIMES
  msgExt.reconsumeTimes = byteBuffer.getInt();
  // 14 Prepared Transaction Offset
  msgExt.preparedTransactionOffset = byteBuffer.getLong().toNumber();
  // 15 BODY
  const bodyLen = byteBuffer.getInt();
  if (bodyLen > 0) {
    if (readBody) {
      let body = new Buffer(bodyLen);
      byteBuffer.get(body);

      // uncompress body
      if (deCompressBody && (sysFlag & MessageSysFlag.CompressedFlag) === MessageSysFlag.CompressedFlag) {
        body = utils.uncompress(body);
      }
      msgExt.body = body;
    } else {
      byteBuffer.position(byteBuffer.position() + bodyLen);
    }
  }

  // 16 TOPIC
  const topicLen = byteBuffer.get();
  const topic = new Buffer(topicLen);
  byteBuffer.get(topic);
  msgExt.topic = topic.toString();

  // 17 properties
  const propertiesLength = byteBuffer.getShort();
  if (propertiesLength > 0) {
    const properties = new Buffer(propertiesLength);
    byteBuffer.get(properties);
    const propertiesString = properties.toString();
    msgExt.properties = string2messageProperties(propertiesString);
  }

  msgExt.msgId = bytes2string(byteBufferMsgId.copy());
  return msgExt;
}

/**
 * 解析一批消息
 * @param {ByteBuffer} byteBuffer -
 * @param {Boolean} readBody - 是否读取 body 部分
 * @return {RemotingCommand} command
 */
exports.decodes = function(byteBuffer, readBody) {
  readBody = readBody || true;
  const msgExts = [];
  let msgExt;
  while (byteBuffer.hasRemaining()) {
    msgExt = decode(byteBuffer, readBody);
    if (msgExt) {
      msgExts.push(msgExt);
    } else {
      break;
    }
  }
  return msgExts;
};

// Helper
// -----------
function string2bytes(hexString) {
  if (!hexString) {
    return null;
  }
  return new Buffer(hexString, 'hex');
}

function bytes2string(src) {
  if (!src) {
    return null;
  }
  return src.toString('hex').toUpperCase();
}

function string2messageProperties(properties) {
  const map = {
    TAGS: null,
  };
  if (properties) {
    const items = properties.split(PROPERTY_SEPARATOR);
    for (const item of items) {
      const index = item.indexOf(NAME_VALUE_SEPARATOR);
      map[item.slice(0, index)] = item.slice(index + 1);
    }
  }
  return map;
}
