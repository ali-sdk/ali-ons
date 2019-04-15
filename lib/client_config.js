'use strict';

const Base = require('sdk-base');
const address = require('address');
const MixAll = require('./mix_all');

const defaultOptions = {
  instanceName: 'DEFAULT',
  pollNameServerInteval: 30 * 1000,
  heartbeatBrokerInterval: 30 * 1000,
  persistConsumerOffsetInterval: 5 * 1000,
  rebalanceInterval: 10 * 1000,
  clientIP: address.ip(),
  unitMode: false,
  // 阿里云自创建实例，需要 ns 前缀
  namespace: '',
  // 公有云生产环境：http://onsaddr-internal.aliyun.com:8080/rocketmq/nsaddr4client-internal
  // 公有云公测环境：http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet
  // 杭州金融云环境：http://jbponsaddr-internal.aliyun.com:8080/rocketmq/nsaddr4client-internal
  // 杭州深圳云环境：http://mq4finance-sz.addr.aliyun.com:8080/rocketmq/nsaddr4client-internal
  onsAddr: 'http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet',
  // https://help.aliyun.com/document_detail/102895.html 阿里云产品更新，支持实例化
  // nameSrv: 'onsaddr.mq-internet-access.mq-internet.aliyuncs.com:80',
  onsChannel: 'ALIYUN', // CLOUD, ALIYUN, ALL
};

class ClientConfig extends Base {

  /**
   * Producer 与 Consumer的公共配置
   * @param {Object} options
   *  - {String} instanceName 示例名称
   *  - {Number} pollNameServerInteval name server 同步的间隔
   *  - {Number} heartbeatBrokerInterval 心跳的间隔
   *  - {Number} persistConsumerOffsetInterval 持久化消费进度的间隔
   *  - {Boolean} unitMode 是否为单元化的订阅组
   */
  constructor(options) {
    super(Object.assign({}, defaultOptions, options));
    this.instanceName = this.options.instanceName;
  }

  get clientId() {
    return `${this.options.clientIP}@${this.instanceName}`;
  }

  get pollNameServerInteval() {
    return this.options.pollNameServerInteval;
  }

  get heartbeatBrokerInterval() {
    return this.options.heartbeatBrokerInterval;
  }

  get persistConsumerOffsetInterval() {
    return this.options.persistConsumerOffsetInterval;
  }

  get rebalanceInterval() {
    return this.options.rebalanceInterval;
  }

  get unitMode() {
    return this.options.unitMode;
  }

  get namespace() {
    return this.options.namespace;
  }

  formatTopic(topic) {
    if (this.namespace && (!topic.startsWith(this.namespace) &&
        !topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))) {
      topic = `${this.namespace}%${topic}`;
    }
    return topic;
  }

  /**
   * 将实例名修改为进程号
   */
  changeInstanceNameToPID() {
    if (this.instanceName === 'DEFAULT') {
      this.instanceName = process.pid + '';
    }
  }
}

module.exports = ClientConfig;
