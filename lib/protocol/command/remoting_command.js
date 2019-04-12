'use strict';
/* eslint no-bitwise: 0*/

const bytes = require('bytes');
const is = require('is-type-of');
const ByteBuffer = require('byte');
const OpaqueGenerator = require('./opaque_generator');

const REQUEST_COMMAND = 'REQUEST_COMMAND';
const RESPONSE_COMMAND = 'RESPONSE_COMMAND';

// 0, REQUEST_COMMAND
// 1, RESPONSE_COMMAND
const RPC_TYPE = 0;
// 0, RPC
// 1, Oneway
const RPC_ONEWAY = 1;

// V3_4_9 支持设置重试的 maxReconsumeTimes
const MQ_VERSION = 121;

const buf = new ByteBuffer({ size: bytes('1m') });

class RemotingCommand {

  /**
   * metaq 命令
   * @param {Object} options
   *  - code {Number} 命令代号
   *  - customHeader {Object} 自定义头部
   *  - flag {Number} 标识命令的属性
   *  - opaque {Number} 关联请求、响应的字段
   *  - remark {String}
   *  - extFields {Object} 扩展字段
   */
  constructor(options) {
    this.code = options.code;
    this.customHeader = options.customHeader;
    this.flag = options.flag || 0;
    this.opaque = options.opaque || OpaqueGenerator.getNextOpaque();
    this.remark = options.remark;
    this.extFields = options.extFields || {};
    this.body = null;
  }

  get language() {
    return 'JAVA';
  }

  get version() {
    return MQ_VERSION;
  }

  get type() {
    return this.isResponseType ? RESPONSE_COMMAND : REQUEST_COMMAND;
  }

  get isResponseType() {
    const bits = 1 << RPC_TYPE;
    return (this.flag & bits) === bits;
  }

  get isOnewayRPC() {
    const bits = 1 << RPC_ONEWAY;
    return (this.flag & bits) === bits;
  }

  markResponseType() {
    const bits = 1 << RPC_TYPE;
    this.flag |= bits;
  }

  markOnewayRPC() {
    const bits = 1 << RPC_ONEWAY;
    this.flag |= bits;
  }

  makeCustomHeaderToNet() {
    const customHeader = this.customHeader;
    if (!customHeader) {
      return;
    }

    this.extFields = this.extFields || {};
    for (const key in customHeader) {
      const field = customHeader[key];
      if (is.nullOrUndefined(field) || is.function(field)) {
        continue;
      }
      this.extFields[key] = field;
    }
  }

  decodeCommandCustomHeader() {
    return JSON.parse(JSON.stringify(this.extFields)); // 深拷贝
  }

  buildHeader() {
    this.makeCustomHeaderToNet();
    return Buffer.from(JSON.stringify({
      code: this.code,
      language: this.language,
      opaque: this.opaque,
      flag: this.flag,
      version: this.version,
      remark: this.remark,
      extFields: this.extFields,
    }), 'utf8');
  }

  encode() {
    buf.reset();
    let length = 4;
    const headerData = this.buildHeader();
    length += headerData.length;

    if (this.body) {
      length += this.body.length;
    }
    buf.putInt(length);
    buf.putInt(headerData.length);
    buf.put(headerData);

    if (this.body) {
      buf.put(this.body);
    }
    return buf.copy();
  }

  /**
   * 创建 request 命令
   * @param {Number} code - 命令代号
   * @param {Object} customHeader -
   * @return {RemotingCommand} command
   */
  static createRequestCommand(code, customHeader) {
    return new RemotingCommand({
      code,
      customHeader,
    });
  }

  /**
   * 创建 request 命令
   * @param {Number} code - 命令代号
   * @param {Number} opaque -
   * @param {String} remark -
   * @return {RemotingCommand} command
   */
  static createResponseCommand(code, opaque, remark) {
    const command = new RemotingCommand({
      code,
      opaque,
      remark: remark || 'not set any response code',
    });
    command.markResponseType();
    return command;
  }

  /**
   * 反序列化报文
   * @param {Buffer} packet -
   * @return {RemotingCommand} command
   */
  static decode(packet) {
    const customHeaderLength = packet.readInt32BE(4);
    const customHeaderData = packet.slice(8, 8 + customHeaderLength);
    let body;
    if (packet.length - 8 - customHeaderLength > 0) {
      body = packet.slice(8 + customHeaderLength);
    }
    const command = new RemotingCommand(JSON.parse(customHeaderData));
    command.body = body;
    return command;
  }
}

module.exports = RemotingCommand;
