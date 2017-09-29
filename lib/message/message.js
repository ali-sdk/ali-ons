'use strict';

const is = require('is-type-of');
const MessageConst = require('./message_const');

class Message {

  /**
   * 创建消息对象
   * @param {String} topic -
   * @param {String} tags -
   * @param {String|Buffer} body -
   * @constructor
   */
  constructor(topic, tags, body) {
    if (arguments.length === 2) {
      body = tags;
      tags = null;
    }

    this.storeSize = null;
    this.bodyCRC = null;
    this.queueId = null;
    this.flag = 0;
    this.queueOffset = null; // long
    this.commitLogOffset = null; // long
    this.bornHost = null;
    this.bornTimestamp = null; // long
    this.storeTimestamp = null; // long
    this.storeHost = null;
    this.reconsumeTimes = 0;
    this.preparedTransactionOffset = null; // long
    this.topic = topic;
    this.properties = {};
    this.msgId = null;

    this.tags = tags;

    if (body && is.string(body)) {
      this.body = new Buffer(body);
    } else {
      this.body = body;
    }
  }

  /**
   * 消息标签，用于过滤
   * @property {String} Message#tags
   */
  get tags() {
    return this.properties && this.properties[MessageConst.PROPERTY_TAGS];
  }

  set tags(val) {
    this.properties[MessageConst.PROPERTY_TAGS] = val;
  }

  /**
   * 消息关键词
   * @property {String} Message#keys
   */
  get keys() {
    return this.properties && this.properties[MessageConst.PROPERTY_KEYS];
  }

  set keys(val) {
    this.properties[MessageConst.PROPERTY_KEYS] = val.join(MessageConst.KEY_SEPARATOR).trim();
  }

  /**
   * 消息延时投递时间级别，0表示不延时，大于0表示特定延时级别（具体级别在服务器端定义）
   * @property {Number} Message#delayTimeLevel
   */
  get delayTimeLevel() {
    const t = this.properties && this.properties[MessageConst.PROPERTY_DELAY_TIME_LEVEL];
    if (t) {
      return parseInt(t, 10);
    }
    return 0;
  }

  set delayTimeLevel(val) {
    this.properties[MessageConst.PROPERTY_DELAY_TIME_LEVEL] = val + '';
  }

  /**
   * 是否等待服务器将消息存储完毕再返回（可能是等待刷盘完成或者等待同步复制到其他服务器）
   * @property {Boolean} Message#waitStoreMsgOK
   */
  get waitStoreMsgOK() {
    const result = this.properties && this.properties[MessageConst.PROPERTY_WAIT_STORE_MSG_OK];
    return !!result;
  }

  set waitStoreMsgOK(val) {
    this.properties[MessageConst.PROPERTY_WAIT_STORE_MSG_OK] = String(!!val);
  }

  /**
   * userId，用于单元化
   * @property {String} Message#buyerId
   */
  get buyerId() {
    return this.properties && this.properties[MessageConst.PROPERTY_BUYER_ID];
  }

  set buyerId(val) {
    this.properties[MessageConst.PROPERTY_BUYER_ID] = val;
  }
}

module.exports = Message;
