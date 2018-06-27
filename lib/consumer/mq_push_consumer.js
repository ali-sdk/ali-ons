'use strict';

const assert = require('assert');
const is = require('is-type-of');
const utils = require('../utils');
const logger = require('../logger');
const MixAll = require('../mix_all');
const MQClient = require('../mq_client');
const sleep = require('mz-modules/sleep');
const ClientConfig = require('../client_config');
const ProcessQueue = require('../process_queue');
const PullStatus = require('../consumer/pull_status');
const PullSysFlag = require('../utils/pull_sys_flag');
const ConsumeFromWhere = require('./consume_from_where');
const MessageModel = require('../protocol/message_model');
const ConsumeType = require('../protocol/consume_type');
const ReadOffsetType = require('../store/read_offset_type');
const LocalFileOffsetStore = require('../store/local_file');
const LocalMemoryOffsetStore = require('../store/local_memory');
const RemoteBrokerOffsetStore = require('../store/remote_broker');
const AllocateMessageQueueAveragely = require('./rebalance/allocate_message_queue_averagely');

const defaultOptions = {
  logger,
  persistent: false, // 是否持久化消费进度
  isBroadcast: false, // 是否是广播模式（默认集群消费模式）
  brokerSuspendMaxTimeMillis: 1000 * 15, // 长轮询模式，Consumer连接在Broker挂起最长时间
  pullTimeDelayMillsWhenException: 3000, // 拉消息异常时，延迟一段时间再拉
  pullTimeDelayMillsWhenFlowControl: 5000, // 进入流控逻辑，延迟一段时间再拉
  consumerTimeoutMillisWhenSuspend: 1000 * 30, // 长轮询模式，Consumer超时时间（必须要大于brokerSuspendMaxTimeMillis）
  consumerGroup: MixAll.DEFAULT_CONSUMER_GROUP,
  consumeFromWhere: ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, // Consumer第一次启动时，从哪里开始消费
  /**
   * Consumer第一次启动时，如果回溯消费，默认回溯到哪个时间点，数据格式如下，时间精度秒：
   * 20131223171201
   * 表示2013年12月23日17点12分01秒
   * 默认回溯到相对启动时间的半小时前
   */
  consumeTimestamp: utils.timeMillisToHumanString(Date.now() - 1000 * 60 * 30),
  pullThresholdForQueue: 500, // 本地队列消息数超过此阀值，开始流控
  pullInterval: 0, // 拉取消息的频率, 如果为了降低拉取速度，可以设置大于0的值
  consumeMessageBatchMaxSize: 1, // 消费一批消息，最大数
  pullBatchSize: 32, // 拉消息，一次拉多少条
  postSubscriptionWhenPull: true, // 是否每次拉消息时，都上传订阅关系
  allocateMessageQueueStrategy: new AllocateMessageQueueAveragely(), // 队列分配算法，应用可重写
};

class MQPushConsumer extends ClientConfig {
  constructor(options) {
    assert(options && options.consumerGroup, '[MQPushConsumer] options.consumerGroup is required');
    super(Object.assign({ initMethod: 'init' }, defaultOptions, options));

    // @example:
    // pullFromWhichNodeTable => {
    //   '[topic="TEST_TOPIC", brokerName="qdinternet-03", queueId="1"]': 0
    // }
    this._pullFromWhichNodeTable = new Map();
    this._subscriptions = new Map();
    this._handles = new Map();
    this._topicSubscribeInfoTable = new Map();
    this._processQueueTable = new Map();
    this._inited = false;
    this._isClosed = false;

    if (this.messageModel === MessageModel.CLUSTERING) {
      this.changeInstanceNameToPID();
    }

    this._mqClient = MQClient.getAndCreateMQClient(this);
    this._offsetStore = this.newOffsetStoreInstance();

    this._mqClient.on('error', err => this._handleError(err));
    this._offsetStore.on('error', err => this._handleError(err));
  }

  get logger() {
    return this.options.logger;
  }

  get subscriptions() {
    return this._subscriptions;
  }

  get processQueueTable() {
    return this._processQueueTable;
  }

  get consumerGroup() {
    return this.options.consumerGroup;
  }

  get messageModel() {
    return this.options.isBroadcast ? MessageModel.BROADCASTING : MessageModel.CLUSTERING;
  }

  get consumeType() {
    return ConsumeType.CONSUME_PASSIVELY;
  }

  get consumeFromWhere() {
    return this.options.consumeFromWhere;
  }

  get allocateMessageQueueStrategy() {
    return this.options.allocateMessageQueueStrategy;
  }

  async init() {
    this._mqClient.registerConsumer(this.consumerGroup, this);
    await this._mqClient.ready();
    await this._offsetStore.load();
    this.logger.info('[mq:consumer] consumer started');
    this._inited = true;
  }

  /**
   * close the consumer
   */
  async close() {
    this._isClosed = true;
    await this.persistConsumerOffset();
    this._pullFromWhichNodeTable.clear();
    this._subscriptions.clear();
    this._topicSubscribeInfoTable.clear();
    this._processQueueTable.clear();

    await this._mqClient.unregisterConsumer(this.consumerGroup);
    await this._mqClient.close();
    this.logger.info('[mq:consumer] consumer closed');
    this.emit('close');
  }

  newOffsetStoreInstance() {
    if (this.messageModel === MessageModel.BROADCASTING) {
      if (this.options.persistent) {
        return new LocalFileOffsetStore(this._mqClient, this.consumerGroup);
      }
      return new LocalMemoryOffsetStore(this._mqClient, this.consumerGroup);
    }
    return new RemoteBrokerOffsetStore(this._mqClient, this.consumerGroup);
  }

  /**
   * subscribe
   * @param {String} topic - topic
   * @param {String} subExpression - tag
   * @param {Function} handler - message handler
   * @return {void}
   */
  subscribe(topic, subExpression, handler) {
    const _self = this;

    if (arguments.length === 2) {
      handler = subExpression;
      subExpression = null;
    }
    assert(is.asyncFunction(handler), '[MQPushConsumer] handler should be a asyncFunction');
    assert(!_self.subscriptions.has(topic), `[MQPushConsumer] ONLY one handler allowed for topic=${topic}`);

    const subscriptionData = _self.buildSubscriptionData(_self.consumerGroup, topic, subExpression);
    const tagsSet = subscriptionData.tagsSet;
    const needFilter = !!tagsSet.length;
    _self.subscriptions.set(topic, {
      handler,
      subscriptionData,
    });

    (async () => {
      try {
        await _self.ready();
        // 如果 topic 没有路由信息，先更新一下
        if (!_self._topicSubscribeInfoTable.has(topic)) {
          await _self._mqClient.updateAllTopicRouterInfo();
          await _self._mqClient.sendHeartbeatToAllBroker();
          await _self._mqClient.doRebalance();
        }

        while (!_self._isClosed && _self.subscriptions.has(topic)) {
          const mqList = _self._topicSubscribeInfoTable.get(topic);
          let hasMsg = false;
          if (mqList && mqList.length) {
            for (const mq of mqList) {
              const item = _self._processQueueTable.get(mq.key);
              if (item) {
                const pq = item.processQueue;
                while (pq.msgCount) {
                  hasMsg = true;
                  const msg = pq.msgList[0];
                  if (!msg.tags || !needFilter || tagsSet.includes(msg.tags)) {
                    try {
                      await handler(msg, mq, pq);
                    } catch (err) {
                      err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
                      _self.emit('error', err);
                      if (_self.messageModel === MessageModel.CLUSTERING) {
                        // TODO: support retry message
                        msg.reconsumeTimes++;
                        await _self._sleep(5000);
                        continue;
                      } else {
                        _self.logger.warn('[MQPushConsumer] BROADCASTING consume message failed, drop it, msgId: %s', msg.msgId);
                      }
                    }
                  } else {
                    _self.logger.debug('[MQPushConsumer] message filter by tags=, msg.tags=%s', subExpression, msg.tags);
                  }
                  const offset = pq.remove();
                  if (offset >= 0) {
                    _self._offsetStore.updateOffset(mq, offset, true);
                  }
                }
              }
            }
          }

          if (!hasMsg) {
            await _self.await(`topic_${topic}_changed`);
          }
        }
      } catch (err) {
        _self._handleError(err);
      }
    })();
  }

  /**
   * construct subscription data
   * @param {String} consumerGroup - consumer group name
   * @param {String} topic - topic
   * @param {String} subString - tag
   * @return {Object} subscription
   */
  buildSubscriptionData(consumerGroup, topic, subString) {
    const subscriptionData = {
      topic,
      subString,
      classFilterMode: false,
      tagsSet: [],
      codeSet: [],
      subVersion: Date.now(),
    };
    if (is.nullOrUndefined(subString) || subString === '*' || subString === '') {
      subscriptionData.subString = '*';
    } else {
      const tags = subString.split('||');
      for (let tag of tags) {
        tag = tag.trim();
        if (tag) {
          subscriptionData.tagsSet.push(tag);
          subscriptionData.codeSet.push(utils.hashCode(tag));
        }
      }
    }
    return subscriptionData;
  }

  async persistConsumerOffset() {
    const mqs = [];
    for (const key of this._processQueueTable.keys()) {
      if (this._processQueueTable.get(key)) {
        mqs.push(this._processQueueTable.get(key).messageQueue);
      }
    }
    await this._offsetStore.persistAll(mqs);
  }

  /**
   * pull message from queue
   * @param {MessageQueue} messageQueue - message queue
   * @return {void}
   */
  pullMessageQueue(messageQueue) {
    const _self = this;
    (async () => {
      while (!_self._isClosed && _self._processQueueTable.has(messageQueue.key)) {
        try {
          await _self.executePullRequestImmediately(messageQueue);
          await _self._sleep(_self.options.pullInterval);
        } catch (err) {
          if (!_self._isClosed) {
            err.name = 'MQConsumerPullMessageError';
            err.message = `[mq:consumer] pull message for queue: ${messageQueue.key}, occurred error: ${err.message}`;
            _self._handleError(err);
            await _self._sleep(_self.options.pullTimeDelayMillsWhenException);
          }
        }
      }
    })();
  }

  /**
   * execute pull message immediately
   * @param {MessageQueue} messageQueue - messageQueue
   * @return {void}
   */
  async executePullRequestImmediately(messageQueue) {
    // close or queue removed
    if (!this._processQueueTable.has(messageQueue.key)) {
      return;
    }
    const pullRequest = this._processQueueTable.get(messageQueue.key);
    const processQueue = pullRequest.processQueue;
    // queue droped
    if (processQueue.droped) {
      return;
    }

    // flow control
    const size = processQueue.msgCount;
    if (size > this.options.pullThresholdForQueue) {
      await this._sleep(this.options.pullTimeDelayMillsWhenFlowControl);
      return;
    }

    processQueue.lastPullTimestamp = Date.now();
    const data = this.subscriptions.get(messageQueue.topic);
    const subscriptionData = data && data.subscriptionData;
    if (!subscriptionData) {
      this.logger.warn('[mq:consumer] execute pull request, but subscriptionData not found, topic: %s, queueId: %s', messageQueue.topic, messageQueue.queueId);
      await this._sleep(this.options.pullTimeDelayMillsWhenException);
      return;
    }

    let commitOffset = 0;
    const subExpression = this.options.postSubscriptionWhenPull ? subscriptionData.subString : null;
    const subVersion = subscriptionData.subVersion;

    // cluster model
    if (MessageModel.CLUSTERING === this.messageModel) {
      const offset = await this._offsetStore.readOffset(pullRequest.messageQueue, ReadOffsetType.READ_FROM_MEMORY);
      if (offset) {
        commitOffset = offset;
      }
    }
    const pullResult = await this.pullKernelImpl(messageQueue, subExpression, subVersion, pullRequest.nextOffset, commitOffset);
    this.updatePullFromWhichNode(messageQueue, pullResult.suggestWhichBrokerId);
    const originOffset = pullRequest.nextOffset;
    // update next pull offset
    pullRequest.nextOffset = pullResult.nextBeginOffset;

    switch (pullResult.pullStatus) {
      case PullStatus.FOUND:
      {
        const pullRT = Date.now() - processQueue.lastPullTimestamp;
        this.logger.info('[MQPushConsumer] pull message success, found new message size: %d, topic: %s, consumerGroup: %s, cost: %dms.',
          pullResult.msgFoundList.length, messageQueue.topic, this.consumerGroup, pullRT);

        // submit to consumer
        processQueue.putMessage(pullResult.msgFoundList);
        this.emit(`topic_${messageQueue.topic}_changed`);
        break;
      }
      case PullStatus.NO_NEW_MSG:
      case PullStatus.NO_MATCHED_MSG:
        this.logger.debug('[mq:consumer] no new message for topic: %s at message queue => %s', subscriptionData.topic, messageQueue.key);
        this.correctTagsOffset(pullRequest);
        break;
      case PullStatus.OFFSET_ILLEGAL:
        this.logger.warn('[mq:consumer] the pull request offset illegal, message queue => %s, the originOffset => %d, pullResult => %j', messageQueue.key, originOffset, pullResult);
        this._offsetStore.updateOffset(messageQueue, pullRequest.nextOffset);
        break;
      default:
        break;
    }
  }

  async pullKernelImpl(messageQueue, subExpression, subVersion, offset, commitOffset) {
    let sysFlag = PullSysFlag.buildSysFlag( //
      commitOffset > 0, // commitOffset
      true, // suspend
      !!subExpression, // subscription
      false // class filter
    );
    const result = await this.findBrokerAddress(messageQueue);
    if (!result) {
      throw new Error(`The broker[${messageQueue.brokerName}] not exist`);
    }

    // Slave不允许实时提交消费进度，可以定时提交
    if (result.slave) {
      sysFlag = PullSysFlag.clearCommitOffsetFlag(sysFlag);
    }

    const requestHeader = {
      consumerGroup: this.consumerGroup,
      topic: messageQueue.topic,
      queueId: messageQueue.queueId,
      queueOffset: offset,
      maxMsgNums: this.options.pullBatchSize,
      sysFlag,
      commitOffset,
      suspendTimeoutMillis: this.options.brokerSuspendMaxTimeMillis,
      subscription: subExpression,
      subVersion,
    };
    return await this._mqClient.pullMessage(result.brokerAddr, requestHeader, this.options.consumerTimeoutMillisWhenSuspend);
  }

  async findBrokerAddress(messageQueue) {
    let findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(
      messageQueue.brokerName, this.recalculatePullFromWhichNode(messageQueue), false);

    if (!findBrokerResult) {
      await this._mqClient.updateTopicRouteInfoFromNameServer(messageQueue.topic);
      findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(
        messageQueue.brokerName, this.recalculatePullFromWhichNode(messageQueue), false);
    }
    return findBrokerResult;
  }

  recalculatePullFromWhichNode(messageQueue) {
    // @example:
    // pullFromWhichNodeTable => {
    //   '[topic="TEST_TOPIC", brokerName="qdinternet-03", queueId="1"]': 0
    // }
    return this._pullFromWhichNodeTable.get(messageQueue.key) || MixAll.MASTER_ID;
  }

  correctTagsOffset(pullRequest) {
    this._offsetStore.updateOffset(pullRequest.messageQueue, pullRequest.nextOffset, true);
  }

  updatePullFromWhichNode(messageQueue, brokerId) {
    this._pullFromWhichNodeTable.set(messageQueue.key, brokerId);
  }

  /**
   * update subscription data
   * @param {String} topic - topic
   * @param {Array} info - info
   * @return {void}
   */
  updateTopicSubscribeInfo(topic, info) {
    if (this._subscriptions.has(topic)) {
      this._topicSubscribeInfoTable.set(topic, info);
    }
  }

  /**
   * whether need update
   * @param {String} topic - topic
   * @return {Boolean} need update?
   */
  isSubscribeTopicNeedUpdate(topic) {
    if (this._subscriptions && this._subscriptions.has(topic)) {
      return !this._topicSubscribeInfoTable.has(topic);
    }
    return false;
  }

  /**
   * rebalance
   * @return {void}
   */
  async doRebalance() {
    for (const topic of this.subscriptions.keys()) {
      await this.rebalanceByTopic(topic);
    }
  }

  async rebalanceByTopic(topic) {
    this.logger.info('[mq:consumer] rebalanceByTopic: %s, messageModel: %s', topic, this.messageModel);
    const mqSet = this._topicSubscribeInfoTable.get(topic); // messageQueue list
    if (!mqSet || !mqSet.length) {
      this.logger.warn('[mq:consumer] doRebalance, %s, but the topic[%s] not exist.', this.consumerGroup, topic);
      return;
    }

    let changed;
    let allocateResult = mqSet;
    if (this.options.isBroadcast) {
      changed = await this.updateProcessQueueTableInRebalance(topic, mqSet);
    } else {
      const cidAll = await this._mqClient.findConsumerIdList(topic, this.consumerGroup);
      this.logger.info('[mq:consumer] rebalance topic: %s, with consumer ids: %j', topic, cidAll);
      if (cidAll && cidAll.length) {
        // 排序
        mqSet.sort(compare);
        cidAll.sort();

        allocateResult = this.allocateMessageQueueStrategy.allocate(this.consumerGroup, this._mqClient.clientId, mqSet, cidAll);
        this.logger.info('[mq:consumer] allocate queue for group: %s, clientId: %s, result: %j', this.consumerGroup, this._mqClient.clientId, allocateResult);
        changed = await this.updateProcessQueueTableInRebalance(topic, allocateResult);
      }
    }
    if (changed) {
      this.logger.info('[mq:consumer] do rebalance and message queue changed, topic: %s, mqSet: %j', topic, allocateResult);
      this.emit(`topic_${topic}_queue_changed`);
    }
  }

  /**
   * update process queue
   * @param {String} topic - topic
   * @param {Array} mqSet - message queue set
   * @return {void}
   */
  async updateProcessQueueTableInRebalance(topic, mqSet) {
    let changed = false;
    // delete unnecessary queue
    for (const key of this._processQueueTable.keys()) {
      const obj = this._processQueueTable.get(key);
      const messageQueue = obj.messageQueue;
      const processQueue = obj.processQueue;

      if (topic === messageQueue.topic) {
        // not found in mqSet, that means the process queue is unnecessary.
        if (!mqSet.some(mq => mq.key === messageQueue.key)) {
          processQueue.droped = true;
          await this.removeProcessQueue(messageQueue);
          changed = true;
        } else if (processQueue.isPullExpired && this.consumeType === ConsumeType.CONSUME_PASSIVELY) {
          processQueue.droped = true;
          await this.removeProcessQueue(messageQueue);
          changed = true;
          this.logger.warn('[MQPushConsumer] BUG doRebalance, %s, remove unnecessary mq=%s, because pull is pause, so try to fixed it',
            this.consumerGroup, messageQueue.key);
        }
      }
    }

    for (const messageQueue of mqSet) {
      if (this._processQueueTable.has(messageQueue.key)) {
        continue;
      }

      const nextOffset = await this.computePullFromWhere(messageQueue);
      // double check
      if (this._processQueueTable.has(messageQueue.key)) {
        continue;
      }

      if (nextOffset >= 0) {
        const processQueue = new ProcessQueue();
        changed = true;
        this._processQueueTable.set(messageQueue.key, {
          messageQueue,
          processQueue,
          nextOffset,
        });
        // start to pull this queue;
        this.pullMessageQueue(messageQueue);

        this.logger.info('[mq:consumer] doRebalance, %s, add a new messageQueue, %j, its nextOffset: %s', this.consumerGroup, messageQueue, nextOffset);
      } else {
        this.logger.warn('[mq:consumer] doRebalance, %s, new messageQueue, %j, has invalid nextOffset: %s', this.consumerGroup, messageQueue, nextOffset);
      }
    }

    return changed;
  }

  /**
   * compute consume offset
   * @param {MessageQueue} messageQueue - message queue
   * @return {Number} offset
   */
  async computePullFromWhere(messageQueue) {
    try {
      const lastOffset = await this._offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
      this.logger.info('[mq:consumer] read lastOffset => %s from store, topic="%s", brokerName="%s", queueId="%s"', lastOffset, messageQueue.topic, messageQueue.brokerName, messageQueue.queueId);

      let result = -1;
      switch (this.consumeFromWhere) {
        case ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
        case ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET:
        case ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET:
        case ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET:
          // 第二次启动，根据上次的消费位点开始消费
          if (lastOffset >= 0) {
            result = lastOffset;
          } else if (lastOffset === -1) { // 第一次启动，没有记录消费位点
            // 重试队列则从队列头部开始
            if (messageQueue.topic.indexOf(MixAll.RETRY_GROUP_TOPIC_PREFIX) === 0) {
              result = 0;
            } else { // 正常队列则从队列尾部开始
              return await this._mqClient.maxOffset(messageQueue);
            }
          }
          break;
        case ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
          // 第二次启动，根据上次的消费位点开始消费
          if (lastOffset >= 0) {
            result = lastOffset;
          } else {
            result = 0;
          }
          break;
        case ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
          // 第二次启动，根据上次的消费位点开始消费
          if (lastOffset >= 0) {
            result = lastOffset;
          } else if (lastOffset === -1) { // 第一次启动，没有记录消费为点
            // 重试队列则从队列尾部开始
            if (messageQueue.topic.indexOf(MixAll.RETRY_GROUP_TOPIC_PREFIX) === 0) {
              return await this._mqClient.maxOffset(messageQueue);
            }
            // 正常队列则从指定时间点开始
            // 时间点需要参数配置
            const timestamp = utils.parseDate(this.options.consumeTimestamp).getTime();
            return await this._mqClient.searchOffset(messageQueue, timestamp);
          }
          break;
        default:
          break;
      }
      this.logger.info('[mq:consumer] computePullFromWhere() messageQueue => %s should read from offset: %s and lastOffset: %s', messageQueue.key, result, lastOffset);
      return result;
    } catch (err) {
      err.mesasge = 'computePullFromWhere() occurred an exception, ' + err.mesasge;
      this._handleError(err);
      return -1;
    }
  }

  /**
   * 移除消费队列
   * @param {MessageQueue} messageQueue - message queue
   * @return {void}
   */
  async removeProcessQueue(messageQueue) {
    const processQueue = this._processQueueTable.get(messageQueue.key);
    this._processQueueTable.delete(messageQueue.key);
    if (processQueue) {
      processQueue.droped = true;
      await this.removeUnnecessaryMessageQueue(messageQueue, processQueue);
      this.logger.info('[mq:consumer] remove unnecessary messageQueue, %s, Droped: %s', messageQueue.key, processQueue.droped);
    }
  }

  /**
   * remove unnecessary queue
   * @param {MessageQueue} messageQueue - message queue
   * @return {void}
   */
  async removeUnnecessaryMessageQueue(messageQueue) {
    await this._offsetStore.persist(messageQueue);
    this._offsetStore.removeOffset(messageQueue);
    // todo: consume later ？
  }

  // * viewMessage(msgId) {
  //   const info = MessageDecoder.decodeMessageId(msgId);
  //   return yield this._mqClient.viewMessage(info.address, Number(info.offset.toString()), 3000);
  // }

  _handleError(err) {
    err.message = 'MQPushConsumer occurred an error ' + err.message;
    this.emit('error', err);
  }

  _sleep(timeout) {
    return sleep(timeout);
  }
}

module.exports = MQPushConsumer;

// Helper
// ------------------
function compare(mqA, mqB) {
  if (mqA.topic === mqB.topic) {
    if (mqA.brokerName === mqB.brokerName) {
      return mqA.queueId - mqB.queueId;
    }
    return mqA.brokerName > mqB.brokerName ? 1 : -1;
  }
  return mqA.topic > mqB.topic ? 1 : -1;
}
