'use strict';
/* eslint no-bitwise: 0 */

const co = require('co');
const utils = require('../utils');
const logger = require('../logger');
const MixAll = require('../mix_all');
const MQClient = require('../mq_client');
const SendStatus = require('./send_status');
const ClientConfig = require('../client_config');
// const PermName = require('../protocol/perm_name');
const MessageConst = require('../message/message_const');
const TopicPublishInfo = require('./topic_publish_info');
const ResponseCode = require('../protocol/response_code');
const MessageSysFlag = require('../utils/message_sys_flag');
const MessageDecoder = require('../message/message_decoder');

const defaultOptions = {
  logger,
  producerGroup: MixAll.DEFAULT_PRODUCER_GROUP,
  createTopicKey: MixAll.DEFAULT_TOPIC,
  defaultTopicQueueNums: 4,
  sendMsgTimeout: 3000,
  compressMsgBodyOverHowmuch: 1024 * 4,
  retryTimesWhenSendFailed: 3,
  retryAnotherBrokerWhenNotStoreOK: false,
  maxMessageSize: 1024 * 128,
};

class MQProducer extends ClientConfig {

  /**
   * metaq message producer
   * @param {Object} options -
   * @constructor
   */
  constructor(options) {
    super(Object.assign({ initMethod: 'init' }, defaultOptions, options));

    this._topicPublishInfoTable = new Map();
    this._inited = false;

    if (this.producerGroup !== MixAll.CLIENT_INNER_PRODUCER_GROUP) {
      this.changeInstanceNameToPID();
    }

    this._mqClient = MQClient.getAndCreateMQClient(this);
    this._mqClient.on('error', err => this._error(err));
  }

  get logger() {
    return this.options.logger;
  }

  get producerGroup() {
    return this.options.producerGroup;
  }

  get createTopicKey() {
    return this.options.createTopicKey;
  }

  get defaultTopicQueueNums() {
    return this.options.defaultTopicQueueNums;
  }

  /**
   * topic list
   * @property {Array} MQProducer#publishTopicList
   */
  get publishTopicList() {
    return this._topicPublishInfoTable.keys();
  }

  /**
   * start the producer
   * @return {void}
   */
  * init() {
    // this._topicPublishInfoTable.set(this.createTopicKey, new TopicPublishInfo());
    this._mqClient.registerProducer(this.producerGroup, this);
    yield this._mqClient.ready();
    this.logger.info('[mq:producer] producer started');
    this._inited = true;
  }

  /**
   * close the producer
   * @return {Promise} promise
   */
  close() {
    return co(function* () {
      this._topicPublishInfoTable.clear();

      yield this._mqClient.unregisterProducer(this.producerGroup);
      yield this._mqClient.close();
      this.removeAllListeners();
      this._inited = false;
      this.logger.info('[mq:producer] producer close');
    }.bind(this));
  }

  /**
   * default error handler
   * @param {Error} err - error
   * @return {void}
   */
  _error(err) {
    setImmediate(() => {
      err.message = 'MQPushConsumer occurred an error' + err.message;
      this.emit('error', err);
    });
  }

  /**
   * update topic info
   * @param {String} topic - topic
   * @param {Object} info - route info
   * @return {void}
   */
  updateTopicPublishInfo(topic, info) {
    if (topic && info) {
      const prev = this._topicPublishInfoTable.get(topic);
      this._topicPublishInfoTable.set(topic, info);
      if (prev) {
        info.sendWhichQueue = prev.sendWhichQueue;
        this.logger.warn('[mq:producer] updateTopicPublishInfo() prev is not null, %j', prev);
      }
    }
  }

  /**
   * is publish topic info need update
   * @param {String} topic - topic
   * @return {Boolean} need update?
   */
  isPublishTopicNeedUpdate(topic) {
    const info = this._topicPublishInfoTable.get(topic);
    return !info || !info.ok;
  }

  /**
   * create topic
   * @param {String} key - TBW102
   * @param {String} newTopic -
   * @param {Number} queueNum - queue number
   * @param {Number} topicSysFlag - 0
   * @return {void}
   */
  // * createTopic(key, newTopic, queueNum, topicSysFlag) {
  //   yield this.ready();

  //   topicSysFlag = topicSysFlag || 0;
  //   const topicRouteData = yield this._mqClient.getTopicRouteInfoFromNameServer(key, 1000 * 3);
  //   const brokerDataList = topicRouteData.brokerDatas;
  //   if (brokerDataList && brokerDataList.length) {
  //     // 排序原因：即使没有配置顺序消息模式，默认队列的顺序同配置的一致。
  //     brokerDataList.sort(compare);

  //     yield brokerDataList.map(brokerData => {
  //       const addr = brokerData.brokerAddrs[MixAll.MASTER_ID];
  //       if (addr) {
  //         const topicConfig = {
  //           topicName: newTopic,
  //           readQueueNums: queueNum,
  //           writeQueueNums: queueNum,
  //           topicSysFlag,
  //           perm: PermName.PERM_READ | PermName.PERM_WRITE,
  //           topicFilterType: 'SINGLE_TAG',
  //           order: false,
  //         };
  //         this.logger.info('[mq:producer] execute createTopic at broker: %s, topic: %s', addr, newTopic);
  //         return this._mqClient.createTopic(addr, key, topicConfig, 1000 * 3);
  //       }
  //       return null;
  //     });
  //   } else {
  //     throw new Error('Not found broker, maybe key is wrong');
  //   }
  // }

  /**
   * send message
   * @param {Message} msg - message object
   * @return {Object} sendResult
   */
  * send(msg) {
    yield this.ready();

    const topicPublishInfo = yield this.tryToFindTopicPublishInfo(msg.topic);
    if (!topicPublishInfo) {
      throw new Error(`no publish router data for topic: ${msg.topic}`);
    }

    const maxTimeout = this.options.sendMsgTimeout + 1000;
    const timesTotal = 1 + this.options.retryTimesWhenSendFailed;
    const beginTimestamp = Date.now();

    const brokersSent = [];
    let times = 0;
    let lastBrokerName;
    let sendResult;
    for (; times < timesTotal && Date.now() - beginTimestamp < maxTimeout; times++) {
      const messageQueue = topicPublishInfo.selectOneMessageQueue(lastBrokerName);
      if (!messageQueue) {
        continue;
      }
      lastBrokerName = messageQueue.brokerName;
      brokersSent[times] = lastBrokerName;
      try {
        sendResult = yield this.sendKernelImpl(msg, messageQueue);
        if (sendResult.sendStatus === SendStatus.SEND_OK || !this.options.retryAnotherBrokerWhenNotStoreOK) {
          break;
        }
      } catch (err) {
        if (err.name === 'MQClientException') {
          switch (err.code) {
            case ResponseCode.TOPIC_NOT_EXIST:
            case ResponseCode.SERVICE_NOT_AVAILABLE:
            case ResponseCode.SYSTEM_ERROR:
            case ResponseCode.NO_PERMISSION:
            case ResponseCode.NO_BUYER_ID:
            case ResponseCode.NOT_IN_CURRENT_UNIT:
              break;
            default:
              throw err;
          }
        }
      }
    }
    if (!sendResult) {
      throw new Error(`Send [${times}] times, still failed, cost [${Date.now() - beginTimestamp}]ms, Topic: ${msg.topic}, BrokersSent: ${JSON.stringify(brokersSent)}`);
    }
    return sendResult;
  }

  /**
   * try to find topic info from name server
   * @param {String} topic - topic
   * @return {Object} route info
   */
  * tryToFindTopicPublishInfo(topic) {
    let topicPublishInfo = this._topicPublishInfoTable.get(topic);
    if (!topicPublishInfo || !topicPublishInfo.ok) {
      this._topicPublishInfoTable.set(topic, new TopicPublishInfo());
      yield this._mqClient.updateTopicRouteInfoFromNameServer(topic);
      topicPublishInfo = this._topicPublishInfoTable.get(topic);
    }

    if (topicPublishInfo.haveTopicRouterInfo || topicPublishInfo && topicPublishInfo.ok) {
      return topicPublishInfo;
    }

    yield this._mqClient.updateTopicRouteInfoFromNameServer(topic, true, this);
    return this._topicPublishInfoTable.get(topic);
  }

  * sendKernelImpl(msg, messageQueue) {
    let brokerAddr = this._mqClient.findBrokerAddressInPublish(messageQueue.brokerName);

    if (!brokerAddr) {
      yield this.tryToFindTopicPublishInfo(messageQueue.topic);
      brokerAddr = this._mqClient.findBrokerAddressInPublish(messageQueue.brokerName);
      if (!brokerAddr) {
        const err = new Error(`The broker[${messageQueue.brokerName}] not exist`);
        err.name = 'MQClientException';
        throw err;
      }
    }

    let sysFlag = 0;
    if (this.tryToCompressMessage(msg)) {
      sysFlag |= MessageSysFlag.CompressedFlag;
    }

    const tranMsg = msg.properties[MessageConst.PROPERTY_TRANSACTION_PREPARED];
    if (tranMsg) {
      sysFlag |= MessageSysFlag.TransactionPreparedType;
    }

    const requestHeader = {
      producerGroup: this.producerGroup,
      topic: msg.topic,
      defaultTopic: this.createTopicKey,
      defaultTopicQueueNums: this.defaultTopicQueueNums,
      queueId: messageQueue.queueId,
      sysFlag,
      bornTimestamp: Date.now(),
      flag: msg.flag,
      properties: MessageDecoder.messageProperties2String(msg.properties),
      reconsumeTimes: 0,
      unitMode: this.unitMode,
    };

    if (requestHeader.topic.indexOf(MixAll.RETRY_GROUP_TOPIC_PREFIX) === 0) {
      const reconsumeTimes = msg.properties[MessageConst.PROPERTY_RECONSUME_TIME];
      if (reconsumeTimes) {
        requestHeader.reconsumeTimes = parseInt(reconsumeTimes, 10);
        delete msg.properties[MessageConst.PROPERTY_RECONSUME_TIME];
      }
    }

    return yield this._mqClient.sendMessage(brokerAddr, messageQueue.brokerName, msg, requestHeader, this.options.sendMsgTimeout);
  }

  /**
   * 尝试压缩消息
   * @param {Message} msg - 消息对象
   * @return {Boolean} 是否压缩
   */
  tryToCompressMessage(msg) {
    const body = msg.body;
    if (body) {
      if (body.length >= this.options.compressMsgBodyOverHowmuch) {
        try {
          const data = utils.compress(body);
          if (data) {
            msg.body = data;
            return true;
          }
        } catch (err) {
          err.name = 'MetaQCompressError';
          this._error(err);
        }
      }
    }
    return false;
  }
}

module.exports = MQProducer;

// // Helper
// // ---------------
// function compare(routerA, routerB) {
//   if (routerA.brokerName > routerB.brokerName) {
//     return 1;
//   } else if (routerA.brokerName < routerB.brokerName) {
//     return -1;
//   }
//   return 0;
// }
