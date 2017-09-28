'use strict';

const co = require('co');
const is = require('is-type-of');
const gather = require('co-gather');
const utility = require('utility');
const MixAll = require('./mix_all');
const MQClientAPI = require('./mq_client_api');
const MessageQueue = require('./message_queue');
const PermName = require('./protocol/perm_name');
const TopicPublishInfo = require('./producer/topic_publish_info');


const instanceTable = new Map();

class MQClient extends MQClientAPI {

  /**
   * metaq client
   * @param {Object} clientConfig -
   * @constructor
   */
  constructor(clientConfig) {
    super(clientConfig.options);

    this._clientConfig = clientConfig;
    this._brokerAddrTable = new Map();
    this._consumerTable = new Map();
    this._producerTable = new Map();
    this._topicRouteTable = new Map();
    // this.API.on('command', this.handleServerRequest.bind(this));
  }

  /**
   * @property {String} MQClient#clientId
   */
  get clientId() {
    return this._clientConfig.clientId;
  }

  /**
   * @property {Number} MQClient#pollNameServerInteval
   */
  get pollNameServerInteval() {
    return this._clientConfig.pollNameServerInteval;
  }

  /**
   * @property {Number} MQClient#heartbeatBrokerInterval
   */
  get heartbeatBrokerInterval() {
    return this._clientConfig.heartbeatBrokerInterval;
  }

  /**
   * @property {Number} MQClient#persistConsumerOffsetInterval
   */
  get persistConsumerOffsetInterval() {
    return this._clientConfig.persistConsumerOffsetInterval;
  }

  /**
   * @property {Number} MQClient#rebalanceInterval
   */
  get rebalanceInterval() {
    return this._clientConfig.rebalanceInterval;
  }

  /**
   * start the client
   */
  * init() {
    yield super.init();
    yield this.updateAllTopicRouterInfo();
    yield this.sendHeartbeatToAllBroker();
    yield this.doRebalance();

    this.startScheduledTask('updateAllTopicRouterInfo', this.pollNameServerInteval);
    this.startScheduledTask('sendHeartbeatToAllBroker', this.heartbeatBrokerInterval);
    this.startScheduledTask('doRebalance', this.rebalanceInterval);
    this.startScheduledTask('persistAllConsumerOffset', this.persistConsumerOffsetInterval);
  }

  * close() {
    if (this._consumerTable.size || this._producerTable.size) {
      return;
    }

    yield super.close();
    // todo:
  }

  /**
   * start a schedule task
   * @param {String} name - method name
   * @param {Number} interval - schedule interval
   * @param {Number} [delay] - delay time interval
   * @return {void}
   */
  startScheduledTask(name, interval, delay) {
    co(function* () {
      yield sleep(delay || interval);
      while (this._inited) {
        try {
          this.logger.info('[mq:client] execute `%s` at %s', name, utility.YYYYMMDDHHmmss());
          yield this[name]();
        } catch (err) {
          this.logger.error(err);
        }
        yield sleep(interval);
      }
    }.bind(this));
  }

  /**
   * regitser consumer
   * @param {String} group - consumer group name
   * @param {Comsumer} consumer - consumer instance
   * @return {void}
   */
  registerConsumer(group, consumer) {
    if (this._consumerTable.has(group)) {
      this.logger.warn('[mq:client] the consumer group [%s] exist already.', group);
      return;
    }
    this._consumerTable.set(group, consumer);
    this.logger.info('[mq:client] new consumer has regitsered, group: %s', group);
  }

  /**
   * unregister consumer
   * @param {String} group - consumer group name
   * @return {void}
   */
  * unregisterConsumer(group) {
    this._consumerTable.delete(group);
    yield this.unregister(null, group);
    this.logger.info('[mq:client] unregister consumer, group: %s', group);
  }

  /**
   * register producer
   * @param {String} group - producer group name
   * @param {Producer} producer - producer
   * @return {void}
   */
  registerProducer(group, producer) {
    if (this._producerTable.has(group)) {
      this.logger.warn('[mq:client] the producer group [%s] exist already.', group);
      return;
    }
    this._producerTable.set(group, producer);
    this.logger.info('[mq:client] new producer has regitsered, group: %s', group);
  }

  /**
   * unregister producer
   * @param {String} group - producer group name
   * @return {void}
   */
  * unregisterProducer(group) {
    this._producerTable.delete(group);
    yield this.unregister(group, null);
    this.logger.info('[mq:client] unregister producer, group: %s', group);
  }

  /**
   * notify all broker that producer or consumer is offline
   * @param {String} producerGroup - producer group name
   * @param {String} consumerGroup - consumer group name
   * @return {void}
   */
  * unregister(producerGroup, consumerGroup) {
    const brokerAddrTable = this._brokerAddrTable;
    for (const brokerName of brokerAddrTable.keys()) {
      const oneTable = brokerAddrTable.get(brokerName);
      if (!oneTable) {
        continue;
      }

      for (const id in oneTable) {
        const addr = oneTable[id];
        if (addr) {
          yield this.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
        }
      }
    }
  }

  /**
   * update all router info
   * @return {void}
   */
  * updateAllTopicRouterInfo() {
    const topics = [];
    // consumer
    for (const groupName of this._consumerTable.keys()) {
      const consumer = this._consumerTable.get(groupName);
      if (!consumer) {
        continue;
      }
      for (const topic of consumer.subscriptions.keys()) {
        topics.push(topic);
      }
    }

    // producer
    for (const groupName of this._producerTable.keys()) {
      const producer = this._producerTable.get(groupName);
      if (!producer) {
        continue;
      }
      for (const topic of producer.publishTopicList) {
        topics.push(topic);
      }
    }

    this.logger.info('[mq:client] try to update all topic route info. topic: %j', topics);

    const ret = yield gather(topics.map(topic => this.updateTopicRouteInfoFromNameServer(topic)));
    ret.forEach(data => {
      if (data.isError) {
        data.error.message = `[mq:client] updateAllTopicRouterInfo occurred error, ${data.error.message}`;
        this.logger.error(data.error);
      }
    });
  }

  /**
   * update topic route info
   * @param {String} topic - topic
   * @param {Boolean} [isDefault] - is default or not
   * @param {Producer} [defaultMQProducer] - producer
   */
  * updateTopicRouteInfoFromNameServer(topic, isDefault, defaultMQProducer) {
    this.logger.info('[mq:client] updateTopicRouteInfoFromNameServer() topic: %s, isDefault: %s', topic, !!isDefault);
    if (isDefault && defaultMQProducer) {
      const topicRouteData = yield this.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.createTopicKey, 3000);
      if (topicRouteData) {
        for (const data of topicRouteData.queueDatas) {
          const queueNums =
            defaultMQProducer.defaultTopicQueueNums < data.readQueueNums ?
              defaultMQProducer.defaultTopicQueueNums : data.readQueueNums;
          data.readQueueNums = queueNums;
          data.writeQueueNums = queueNums;
        }
        this._refreshTopicRouteInfo(topic, topicRouteData);
      }
    } else {
      const topicRouteData = yield this.getTopicRouteInfoFromNameServer(topic, 3000);
      if (topicRouteData) {
        this._refreshTopicRouteInfo(topic, topicRouteData);
      }
    }
  }

  _refreshTopicRouteInfo(topic, topicRouteData) {
    if (!topicRouteData) {
      return;
    }

    // @example
    // topicRouteData => {
    //   "brokerDatas": [{
    //     "brokerAddrs": {
    //       "0": "10.218.145.166:10911"
    //     },
    //     "brokerName": "taobaodaily-02"
    //   }],
    //   "filterServerTable": {},
    //   "queueDatas": [{
    //     "brokerName": "taobaodaily-02",
    //     "perm": 6,
    //     "readQueueNums": 8,
    //     "topicSynFlag": 0,
    //     "writeQueueNums": 8
    //   }]
    // }
    const prev = this._topicRouteTable.get(topic);
    const needUpdate = this._isRouteDataChanged(prev, topicRouteData) || this._isNeedUpdateTopicRouteInfo(topic);

    this.logger.info('[mq:client] refresh route data for topic: %s, route data: %j, needUpdate: %s', topic, topicRouteData, needUpdate);
    if (!needUpdate) {
      return;
    }

    // @example:
    // this.brokerAddrTable => {
    //   "taobaodaily-02": {
    //     "0": "10.218.145.166:10911"
    //   }
    // }
    for (const brokerData of topicRouteData.brokerDatas) {
      this._brokerAddrTable.set(brokerData.brokerName, brokerData.brokerAddrs);
    }

    // update producer route data
    const publishInfo = this._topicRouteData2TopicPublishInfo(topic, topicRouteData);
    publishInfo.haveTopicRouterInfo = true;
    for (const groupName of this._producerTable.keys()) {
      const producer = this._producerTable.get(groupName);
      if (producer) {
        producer.updateTopicPublishInfo(topic, publishInfo);
      }
    }

    // 更新订阅队列信息
    const subscribeInfo = this._topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
    for (const groupName of this._consumerTable.keys()) {
      const consumer = this._consumerTable.get(groupName);
      if (consumer) {
        consumer.updateTopicSubscribeInfo(topic, subscribeInfo);
      }
    }

    this.logger.info('[mq:client] update topicRouteTable for topic: %s, with topicRouteData: %j', topic, topicRouteData);
    this._topicRouteTable.set(topic, topicRouteData);
  }

  _isRouteDataChanged(prev, current) {
    if (is.nullOrUndefined(prev) || is.nullOrUndefined(current)) {
      return true;
    }
    // todo: performance enhance ?
    return JSON.stringify(prev) !== JSON.stringify(current);
  }

  _isNeedUpdateTopicRouteInfo(topic) {
    // producer
    for (const groupName of this._producerTable.keys()) {
      const producer = this._producerTable.get(groupName);
      if (producer && producer.isPublishTopicNeedUpdate(topic)) {
        return true;
      }
    }

    // consumer
    for (const groupName of this._consumerTable.keys()) {
      const consumer = this._consumerTable.get(groupName);
      if (consumer && consumer.isSubscribeTopicNeedUpdate(topic)) {
        return true;
      }
    }
    return false;
  }

  _topicRouteData2TopicPublishInfo(topic, topicRouteData) {
    const info = new TopicPublishInfo();
    // 顺序消息
    if (topicRouteData.orderTopicConf && topicRouteData.orderTopicConf.length) {
      const brokers = topicRouteData.orderTopicConf.split(';');
      for (const broker of brokers) {
        const item = broker.split(':');
        const nums = parseInt(item[1], 10);
        for (let i = 0; i < nums; i++) {
          info.messageQueueList.push(new MessageQueue(topic, item[0], i));
        }
      }
      info.orderTopic = true;
    } else { // 非顺序消息
      for (const queueData of topicRouteData.queueDatas) {
        if (PermName.isWriteable(queueData.perm)) {
          const brokerData = topicRouteData.brokerDatas.find(data => {
            return data.brokerName === queueData.brokerName;
          });
          if (!brokerData || !brokerData.brokerAddrs[MixAll.MASTER_ID]) {
            continue;
          }
          for (let i = 0, nums = queueData.writeQueueNums; i < nums; i++) {
            info.messageQueueList.push(new MessageQueue(topic, queueData.brokerName, i));
          }
        }
      }
      info.orderTopic = false;
    }
    return info;
  }

  _topicRouteData2TopicSubscribeInfo(topic, topicRouteData) {
    const messageQueueList = [];
    for (const queueData of topicRouteData.queueDatas) {
      if (PermName.isReadable(queueData.perm)) {
        for (let i = 0, nums = queueData.readQueueNums; i < nums; i++) {
          messageQueueList.push(new MessageQueue(topic, queueData.brokerName, i));
        }
      }
    }
    return messageQueueList;
  }

  /**
   * send heartbeat to all brokers
   * @return {void}
   */
  * sendHeartbeatToAllBroker() {
    this._cleanOfflineBroker();

    const heartbeatData = this._prepareHeartbeatData();
    const consumerEmpty = heartbeatData.consumerDataSet.length === 0;
    const producerEmpty = heartbeatData.producerDataSet.length === 0;
    if (consumerEmpty && producerEmpty) {
      this.logger.info('[mq:client] sending hearbeat, but no consumer and no producer');
      return;
    }

    const brokers = [];
    for (const brokerName of this._brokerAddrTable.keys()) {
      const oneTable = this._brokerAddrTable.get(brokerName);
      if (!oneTable) {
        continue;
      }

      for (const id in oneTable) {
        const addr = oneTable[id];
        if (!addr) {
          continue;
        }

        // 说明只有Producer，则不向Slave发心跳
        if (consumerEmpty && Number(id) !== MixAll.MASTER_ID) {
          continue;
        }

        brokers.push(addr);
      }
    }
    const ret = yield gather(brokers.map(addr => this.sendHearbeat(addr, heartbeatData, 3000)));
    this.logger.info('[mq:client] send heartbeat: %j to : %j, and result: %j', heartbeatData, brokers, ret);
  }

  _prepareHeartbeatData() {
    const heartbeatData = {
      clientID: this.clientId,
      consumerDataSet: [],
      producerDataSet: [],
    };

    // Consumer
    for (const groupName of this._consumerTable.keys()) {
      const consumer = this._consumerTable.get(groupName);
      if (consumer) {
        const subscriptionDataSet = [];
        for (const topic of consumer.subscriptions.keys()) {
          const data = consumer.subscriptions.get(topic);
          if (data && data.subscriptionData) {
            subscriptionDataSet.push(data.subscriptionData);
          }
        }

        heartbeatData.consumerDataSet.push({
          groupName: consumer.consumerGroup,
          consumeType: consumer.consumeType,
          messageModel: consumer.messageModel,
          consumeFromWhere: consumer.consumeFromWhere,
          subscriptionDataSet,
          unitMode: consumer.unitMode,
        });
      }
    }

    // Producer
    for (const groupName of this._producerTable.keys()) {
      const producer = this._producerTable.get(groupName);
      if (producer) {
        heartbeatData.producerDataSet.push({
          groupName,
        });
      }
    }
    return heartbeatData;
  }

  _cleanOfflineBroker() {
    for (const brokerName of this._brokerAddrTable.keys()) {
      const oneTable = this._brokerAddrTable.get(brokerName);
      let exists = false;

      for (const brokerId in oneTable) {
        const addr = oneTable[brokerId];
        if (this._isBrokerAddrExistInTopicRouteTable(addr)) {
          exists = true;
        } else {
          delete oneTable[brokerId];
          this.logger.info('[mq:client] the broker addr[%s %s] is offline, remove it', brokerName, addr);
        }
      }

      if (!exists) {
        this._brokerAddrTable.delete(brokerName);
        this.logger.info('[mq:client] the broker[%s] name\'s host is offline, remove it', brokerName);
      }
    }
  }

  _isBrokerAddrExistInTopicRouteTable(addr) {
    for (const topic of this._topicRouteTable.keys()) {
      const topicRouteData = this._topicRouteTable.get(topic);
      for (const brokerData of topicRouteData.brokerDatas) {
        for (const brokerId in brokerData.brokerAddrs) {
          if (brokerData.brokerAddrs[brokerId] === addr) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * rebalance
   * @return {void}
   */
  * doRebalance() {
    for (const groupName of this._consumerTable.keys()) {
      const consumer = this._consumerTable.get(groupName);
      if (consumer) {
        yield consumer.doRebalance();
      }
    }
  }

  /**
   * get all consumer list of topic
   * @param {String} topic - topic
   * @param {String} group - consumer group
   * @return {Array} consumer list
   */
  * findConsumerIdList(topic, group) {
    let brokerAddr = this.findBrokerAddrByTopic(topic);
    if (!brokerAddr) {
      yield this.updateTopicRouteInfoFromNameServer(topic);
      brokerAddr = this.findBrokerAddrByTopic(topic);

      if (!brokerAddr) {
        throw new Error(`The broker of topic[${topic}] not exist`);
      } else {
        return yield this.getConsumerIdListByGroup(brokerAddr, group, 3000);
      }
    } else {
      return yield this.getConsumerIdListByGroup(brokerAddr, group, 3000);
    }
  }

  // get broker address by topic
  findBrokerAddrByTopic(topic) {
    const topicRouteData = this._topicRouteTable.get(topic);
    if (topicRouteData) {
      const brokerDatas = topicRouteData.brokerDatas || [];
      const broker = brokerDatas[0];
      if (broker) {
        // master first, not found try slave
        const addr = broker.brokerAddrs[MixAll.MASTER_ID];
        if (!addr) {
          for (const id in broker.brokerAddrs) {
            return broker.brokerAddrs[id];
          }
        }
        return addr;
      }
    }
    return null;
  }

  /**
   * find broker address
   * @param {String} brokerName - broker name
   * @return {Object} broker info
   */
  findBrokerAddressInAdmin(brokerName) {
    const map = this._brokerAddrTable.get(brokerName);
    if (!map) {
      return null;
    }
    if (map[MixAll.MASTER_ID]) {
      return {
        brokerAddr: map[MixAll.MASTER_ID],
        slave: false,
      };
    }
    for (const id in map) {
      if (map[id]) {
        return {
          brokerAddr: map[id],
          slave: true,
        };
      }
    }
    return null;
  }

  findBrokerAddressInSubscribe(brokerName, brokerId, onlyThisBroker) {
    let brokerAddr = null;
    let slave = false;
    let found = false;
    const map = this._brokerAddrTable.get(brokerName);

    if (map) {
      brokerAddr = map[brokerId];
      slave = Number(brokerId) !== MixAll.MASTER_ID;
      found = !is.nullOrUndefined(brokerAddr);

      // 尝试寻找其他Broker
      if (!found && !onlyThisBroker) {
        for (const id in map) {
          brokerAddr = map[id];
          slave = Number(id) !== MixAll.MASTER_ID;
          found = true;
          break;
        }
      }
    }
    if (found) {
      return {
        brokerAddr,
        slave,
      };
    }
    return null;
  }


  /**
   * get current offset of message queue
   * @param {MessageQueue} messageQueue - message queue
   * @return {Number} offset
   */
  * maxOffset(messageQueue) {
    let brokerAddr = this.findBrokerAddressInPublish(messageQueue.brokerName);
    if (!brokerAddr) {
      yield this.updateTopicRouteInfoFromNameServer(messageQueue.topic);
      brokerAddr = this.findBrokerAddressInPublish(messageQueue.brokerName);

      if (!brokerAddr) {
        throw new Error(`The broker[${messageQueue.brokerName}] not exist`);
      } else {
        return yield this.getMaxOffset(brokerAddr, messageQueue.topic, messageQueue.queueId, 3000);
      }
    } else {
      return yield this.getMaxOffset(brokerAddr, messageQueue.topic, messageQueue.queueId, 3000);
    }
  }

  // should be master
  findBrokerAddressInPublish(brokerName) {
    const map = this._brokerAddrTable.get(brokerName);
    return map && map[MixAll.MASTER_ID];
  }

  * persistAllConsumerOffset() {
    for (const groupName of this._consumerTable.keys()) {
      const consumer = this._consumerTable.get(groupName);
      if (consumer) {
        yield consumer.persistConsumerOffset();
      }
    }
  }

  * searchOffset(messageQueue, timestamp) {
    let brokerAddr = this.findBrokerAddressInPublish(messageQueue.brokerName);
    if (!brokerAddr) {
      yield this.updateTopicRouteInfoFromNameServer(messageQueue.topic);
      brokerAddr = this.findBrokerAddressInPublish(messageQueue.brokerName);

      if (!brokerAddr) {
        throw new Error('The broker[' + messageQueue.brokerName + '] not exist');
      } else {
        return yield super.searchOffset(brokerAddr, messageQueue.topic, messageQueue.queueId, timestamp, 3000);
      }
    } else {
      return yield super.searchOffset(brokerAddr, messageQueue.topic, messageQueue.queueId, timestamp, 3000);
    }
  }

  static getAndCreateMQClient(clientConfig) {
    const clientId = clientConfig.clientId;
    let instance = instanceTable.get(clientId);
    if (!instance) {
      instance = new MQClient(clientConfig);
      instanceTable.set(clientId, instance);
      instance.once('close', () => {
        instanceTable.delete(clientId);
      });
    }
    return instance;
  }
}

module.exports = MQClient;

// Helper
// -----------------
function sleep(interval) {
  return callback => setTimeout(callback, interval);
}
