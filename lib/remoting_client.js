'use strict';

const assert = require('assert');
const Base = require('sdk-base');
const logger = require('./logger');
const Channel = require('./channel');

const defaultOptions = {
  logger,
  responseTimeout: 3000,
};

class RemotingClient extends Base {

  /**
   * rocketmq remoting client
   * @param {Object} options
   *   - {HttpClient} urllib - http request client
   *   - {Object} [logger] - log module
   *   - {Number} [responseTimeout] - tcp response timeout
   * @constructor
   */
  constructor(options) {
    assert(options.onsAddr, '[RemotingClient] options.onsAddr is required');
    assert(options.urllib, '[RemotingClient] options.urllib is required');
    super(Object.assign({ initMethod: 'init' }, defaultOptions, options));

    this._inited = false;
    this._namesrvAddrList = [];
    this._channels = new Map();
  }

  /**
   * @property {HttpClient} RemotingClient#urllib
   */
  get urllib() {
    return this.options.urllib;
  }

  /**
   * @property {Object} RemotingClient#logger
   */
  get logger() {
    return this.options.logger;
  }

  /**
   * @property {Number} RemotingClient#responseTimeout
   */
  get responseTimeout() {
    return this.options.responseTimeout;
  }

  /**
   * start the client
   * @return {void}
   */
  * init() {
    // get name server address at first
    yield this.updateNameServerAddressList();
    this._inited = true;
    this.logger.info('[mq:remoting_client] remoting client started');
  }

  /**
   * close the client
   * @return {void}
   */
  * close() {
    if (!this._inited) {
      return;
    }

    // wait all channel close
    yield Promise.all(Array.from(this._channels.keys()).map(addr => {
      return new Promise(resolve => {
        const channel = this._channels.get(addr);
        if (channel && channel.isOK) {
          channel.once('close', resolve);
          channel.close();
        } else {
          resolve();
        }
        this._channels.delete(addr);
      });
    }));

    this._inited = false;
    this.emit('close');
    this.removeAllListeners();

    this.logger.info('[mq:remoting_client] remoting client is closed');
  }

  /**
   * default error handler
   * @param {Error} err - error object
   * @return {void}
   */
  error(err) {
    this.emit('error', err);
  }

  * handleClose(addr, channel) {
    if (this._channels.get(addr) && this._channels.get(addr).clientId === channel.clientId) {
      this._channels.delete(addr);
    }
    // refresh latest server list
    yield this.updateNameServerAddressList();
  }

  * getNameServerAddressList() {
    if (this.options.namesrvAddr) {
      return this.options.namesrvAddr;
    }
    const ret = yield this.urllib.request(this.options.onsAddr);

    if (ret.status === 200) {
      return ret.data.toString().trim();
    }
    throw new Error('[mq:remoting_client] fetch name server addresses failed, ret.statusCode: ' + ret.status);
  }

  /**
   * fetch name server address list
   * @return {void}
   */
  * updateNameServerAddressList() {
    const addrs = yield this.getNameServerAddressList();
    const newList = addrs.split(';');
    if (newList.length) {
      this._namesrvAddrList = newList;
    }
    this.logger.info('[mq:remoting_client] fetch name server addresses successfully, address: %s', addrs);
  }

  /**
   * invoke command
   * @param {String} addr - server address
   * @param {RemotingCommand} command - remoting command
   * @param {Number} [timeout] - response timeout
   * @return {Object} response
   */
  * invoke(addr, command, timeout) {
    if (!this._inited) {
      yield this.ready();
    }
    return yield this.getAndCreateChannel(addr)
      .invoke(command, timeout || this.responseTimeout);
  }

  /**
   * invoke command without response
   * @param {String} addr - server address
   * @param {RemotingCommand} command - remoting command
   * @return {void}
   */
  * invokeOneway(addr, command) {
    if (!this._inited) {
      yield this.ready();
    }
    yield this.getAndCreateChannel(addr).invokeOneway(command);
  }

  /**
   * get request channel from address
   * @param {String} [addr] - server address
   * @return {Channel} request channel
   */
  getAndCreateChannel(addr) {
    if (!addr) {
      const index = Math.floor(Math.random() * this._namesrvAddrList.length);
      addr = this._namesrvAddrList[index];
    }
    let channel = this._channels.get(addr);
    if (channel && channel.isOK) {
      return channel;
    }
    channel = new Channel(addr, this.options);
    this._channels.set(addr, channel);
    channel.once('close', this.handleClose.bind(this, addr, channel));
    channel.on('error', err => this.error(err));
    channel.on('request', (request, address) => this.emit('request', request, address));
    return channel;
  }
}

module.exports = RemotingClient;
