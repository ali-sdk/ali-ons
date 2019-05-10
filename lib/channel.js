'use strict';

const is = require('is-type-of');
const crypto = require('crypto');
const Base = require('tcp-base');
const promisify = require('util').promisify;
const RemotingCommand = require('./protocol/command/remoting_command');

class Channel extends Base {
  /**
   * rocketmq tcp channel object
   * @param {String} address - server address
   * @param {Object} options
   *   - {String} accessKey
   *   - {String} secretKey
   *   - {String} onsChannel
   * @class
   */
  constructor(address, options = {}) {
    // 10.18.214.201:8080
    const arr = address.split(':');
    // support alias: accessKeyId and accessKeySecret
    options.accessKey = options.accessKey || options.accessKeyId;
    options.secretKey = options.secretKey || options.accessKeySecret;
    super(Object.assign({
      host: arr[0],
      port: arr[1],
      headerLength: 4,
      needHeartbeat: false,
    }, options));
    this.sendPromise = promisify(this.send);
  }

  get accessKey() {
    return this.options.accessKey;
  }

  get secretKey() {
    return this.options.secretKey;
  }

  get onsChannel() {
    return 'ALIYUN';
  }

  /**
   * Get packet length from header
   * @param {Buffer} header - packet header
   * @return {Number} bodyLength
   */
  getBodyLength(header) {
    return header.readInt32BE(0);
  }

  decode(body, header) {
    const command = RemotingCommand.decode(Buffer.concat([ header, body ]));
    return {
      id: command.opaque,
      isResponse: command.isResponseType,
      data: command,
    };
  }

  beforeRequest(command) {
    if (!this.accessKey || !this.secretKey) {
      return;
    }

    const header = command.customHeader;
    const map = new Map();
    map.set('AccessKey', this.accessKey);
    map.set('OnsChannel', this.onsChannel);
    if (header) {
      for (const field in header) {
        if (!is.nullOrUndefined(header[field])) {
          map.set(field, header[field].toString());
        }
      }
    }

    let val = '';
    const fields = Array.from(map.keys()).sort();
    for (const key of fields) {
      if (key !== 'Signature') {
        val += map.get(key);
      }
    }
    let total = Buffer.from(val, 'utf8');
    const bodyLength = command.body ? command.body.length : 0;
    if (bodyLength) {
      total = Buffer.concat([ total, command.body ], total.length + bodyLength);
    }
    const hmac = crypto.createHmac('sha1', this.secretKey);
    const signature = hmac.update(total).digest('base64');
    command.extFields.Signature = signature;
    command.extFields.AccessKey = this.accessKey;
    command.extFields.OnsChannel = this.onsChannel;
  }

  /**
   * invoke rocketmq api
   * @param {RemotingCommand} command - remoting command
   * @param {Number} timeout - response timeout
   * @return {Object} response
   */
  invoke(command, timeout) {
    this.beforeRequest(command);
    return this.sendPromise({
      id: command.opaque,
      data: command.encode(),
      timeout,
    }).catch(err => {
      // TODO: not sure whether is work ?
      if (err.name === 'ResponseTimeoutError') {
        this.close();
      }
      throw err;
    });
  }

  /**
   * invoke rocketmq api without need response
   * @param {RemotingCommand} command - remoting command
   * @return {Promise} Promise
   */
  invokeOneway(command) {
    this.beforeRequest(command);
    return this.sendPromise({
      id: command.opaque,
      data: command.encode(),
      oneway: true,
    });
  }
}

module.exports = Channel;
