'use strict';
/* eslint no-bitwise: 0 */

const address = require('address');
const utils = require('../utils');
const ByteBuffer = require('byte');
const MessageConst = require('./message_const');
const { bytes2string } = require('./message_helper');

let FIX_STRING;
let COUNTER = 0;
let startTime;
let nextStartTime;

(function() {
  const tempBuffer = ByteBuffer.allocate(10);
  tempBuffer.position(2);
  tempBuffer.putInt(process.pid);
  tempBuffer.position(0);
  try {
    const ip = Buffer.from(address.ip().split('.').map(v => parseInt(v, 10)));
    if (ip) {
      tempBuffer.put(ip);
    } else {
      tempBuffer.put(createFakeIP());
    }
  } catch (e) {
    tempBuffer.put(createFakeIP());
  }
  tempBuffer.position(6);
  tempBuffer.putInt(utils.hashCode(Math.random().toString()));
  FIX_STRING = bytes2string(tempBuffer.copy());
  setStartTime(new Date().getTime());
})();

function createFakeIP() {
  const bb = ByteBuffer.allocate(8);
  bb.putLong(new Date().getTime());
  bb.position(4);
  const fakeIP = Buffer.alloc(4);
  bb.get(fakeIP);
  return fakeIP;
}

function setStartTime(millis) {
  const date = new Date(millis);
  startTime = new Date(date.getFullYear(), date.getMonth(), 1).getTime();
  nextStartTime = new Date(date.getFullYear(), date.getMonth() + 1, 1).getTime();
}

function createUniqID() {
  return FIX_STRING + bytes2string(createUniqIDBuffer());
}

function createUniqIDBuffer() {
  const buffer = ByteBuffer.allocate(4 + 2);
  const current = new Date().getTime();
  if (current >= nextStartTime) {
    setStartTime(current);
  }
  buffer.position(0);
  buffer.putInt(new Date().getTime() - startTime);
  COUNTER = (COUNTER + 1) & 0xFFFF;
  buffer.putUInt16(COUNTER);
  return buffer.copy();
}

function setUniqID(msg) {
  if (!msg.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX]) {
    const uniqId = createUniqID();
    msg.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX] = uniqId;
  }
}

function getUniqID(msg) {
  return msg.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX];
}

module.exports = {
  setUniqID,
  getUniqID,
};

