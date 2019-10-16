'use strict';

const assert = require('assert');
const Base = require('sdk-base');
const logger = require('./logger');
const Channel = require('./channel');

const defaultOptions = {
  logger,
  responseTimeout: 30000,
};

class RemotingClient extends Base {

  /**
   * rocketmq remoting client
   * @param {Object} options
   *   - {HttpClient} httpclient - http request client
   *   - {Object} [logger] - log module
   *   - {Number} [responseTimeout] - tcp response timeout
   * @class
   */
  constructor(options) {
    assert(options.onsAddr || options.nameSrv, '[RemotingClient] options.onsAddr or options.nameSrv one of them is required');
    assert(options.httpclient, '[RemotingClient] options.httpclient is required');
    super(Object.assign({ initMethod: 'init' }, defaultOptions, options));
    this._index = 0;
    this._inited = false;
    this._namesrvAddrList = [];
    this._channels = new Map();
  }

  /**
   * @property {HttpClient} RemotingClient#httpclient
   */
  get httpclient() {
    return this.options.httpclient;
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
  async init() {
    // get name server address at first
    await this.updateNameServerAddressList();
    this._inited = true;
    this.logger.info('[mq:remoting_client] remoting client started');
  }

  /**
   * close the client
   * @return {void}
   */
  async close() {
    if (!this._inited) {
      return;
    }

    // wait all channel close
    await Promise.all(Array.from(this._channels.keys()).map(addr => {
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

  async handleClose(addr, channel) {
    if (this._channels.get(addr) && this._channels.get(addr).clientId === channel.clientId) {
      this._channels.delete(addr);
    }
    // refresh latest server list
    await this.updateNameServerAddressList();
  }

  /**
   * fetch name server address list
   * @return {void}
   */
  async updateNameServerAddressList() {
    if (this.options.nameSrv) {
      this._namesrvAddrList = Array.isArray(this.options.nameSrv) ? this.options.nameSrv : [ this.options.nameSrv ];
      return;
    }
    const ret = await this.httpclient.request(this.options.onsAddr, {
      timeout: this.options.connectTimeout || 10000,
    });
    if (ret.status === 200) {
      const addrs = ret.data.toString().trim();
      const newList = addrs.split(';');
      if (newList.length) {
        this._namesrvAddrList = newList;
      }
      this.logger.info('[mq:remoting_client] fetch name server addresses successfully, address: %s', addrs);
    } else {
      throw new Error('[mq:remoting_client] fetch name server addresses failed, ret.statusCode: ' + ret.status);
    }
  }

  /**
   * invoke command
   * @param {String} addr - server address
   * @param {RemotingCommand} command - remoting command
   * @param {Number} [timeout] - response timeout
   * @return {Object} response
   */
  async invoke(addr, command, timeout) {
    if (!this._inited) {
      await this.ready();
    }
    return await this.getAndCreateChannel(addr)
      .invoke(command, timeout || this.responseTimeout);
  }

  /**
   * invoke command without response
   * @param {String} addr - server address
   * @param {RemotingCommand} command - remoting command
   * @return {void}
   */
  async invokeOneway(addr, command) {
    if (!this._inited) {
      await this.ready();
    }
    await this.getAndCreateChannel(addr).invokeOneway(command);
  }

  /**
   * get request channel from address
   * @param {String} [addr] - server address
   * @return {Channel} request channel
   */
  getAndCreateChannel(addr) {
    if (!addr) {
      this._index = ++this._index % this._namesrvAddrList.length;
      addr = this._namesrvAddrList[this._index];
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
