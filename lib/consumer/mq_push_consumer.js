'use strict';

const assert = require('assert');
const is = require('is-type-of');
const utils = require('../utils');
const logger = require('../logger');
const MixAll = require('../mix_all');
const MQClient = require('../mq_client');
const MQProducer = require('../producer/mq_producer');
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
const Message = require('../message/message');
const MessageConst = require('../message/message_const');

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
  consumeOrderly: false, // 是否分区顺序消费
  pullThresholdForQueue: 500, // 本地队列消息数超过此阀值，开始流控
  pullInterval: 0, // 拉取消息的频率, 如果为了降低拉取速度，可以设置大于0的值
  consumeMessageBatchMaxSize: 1, // 消费一批消息，最大数
  pullBatchSize: 32, // 拉消息，一次拉多少条
  parallelConsumeLimit: 1, // 并发消费消息限制
  postSubscriptionWhenPull: true, // 是否每次拉消息时，都上传订阅关系
  allocateMessageQueueStrategy: new AllocateMessageQueueAveragely(), // 队列分配算法，应用可重写
  maxReconsumeTimes: 16, // 最大重试次数,
  suspendCurrentQueueTimeMillis: 1000, // 顺序消费再次消费间隔
  lockInterval: 20000, // 顺序消费循环锁定间隔
};

class MQPushConsumer extends ClientConfig {
  constructor(options) {
    assert(options && options.consumerGroup, '[MQPushConsumer] options.consumerGroup is required');
    const mergedOptions = Object.assign({ initMethod: 'init' }, defaultOptions, options);
    assert(mergedOptions.parallelConsumeLimit <= mergedOptions.pullBatchSize,
      '[MQPushConsumer] options.parallelConsumeLimit must lte pullBatchSize');
    super(mergedOptions);

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

  get parallelConsumeLimit() {
    return this.options.parallelConsumeLimit;
  }

  get consumerGroup() {
    if (this.namespace) {
      return `${this.namespace}%${this.options.consumerGroup}`;
    }
    return this.options.consumerGroup;
  }

  get consumeOrderly() {
    return this.options.consumeOrderly === true;
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
    await MQProducer.getDefaultProducer(this.options);
    await this._mqClient.ready();
    await this._offsetStore.load();
    this.logger.info('[mq:consumer] consumer started');
    this._inited = true;

    // 订阅重试 TOPIC & 顺序队列锁定
    if (this.messageModel === MessageModel.CLUSTERING) {
      if (this.consumeOrderly) {
        this.startScheduledTask('lockAll', this.options.lockInterval, 1000);
      }
      const retryTopic = MixAll.getRetryTopic(this.consumerGroup);
      this.subscribe(retryTopic, '*', async msg => {
        const originTopic = msg.retryTopic;
        const originMsgId = msg.originMessageId;
        const subscription = this._subscriptions.get(originTopic) || {};
        const handler = subscription.handler;
        if (!MixAll.isRetryTopic(originTopic) && handler) {
          await handler(msg);
        } else {
          this.logger.warn('[MQPushConsumer] retry message no handler, originTopic: %s, originMsgId: %s, msgId: %s',
            originTopic,
            originMsgId,
            msg.msgId);
        }
      });
    }
  }

  /**
   * close the consumer
   */
  async close() {
    this._isClosed = true;
    await this.unlockAll();
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

  /**
   * start a schedule task
   * @param {String} name - method name
   * @param {Number} interval - schedule interval
   * @param {Number} [delay] - delay time interval
   * @return {void}
   */
  startScheduledTask(name, interval, delay) {
    (async () => {
      await this._sleep(delay || interval);
      while (this._inited) {
        try {
          await this[name]();
        } catch (err) {
          this.logger.error(err);
        }
        await this._sleep(interval);
      }
    })();
  }

  // 按照brokerName归并MessageQueue集合
  buildProcessQueueTableByBrokerName() {
    const brokerMqs = new Map();
    for (const key of this._processQueueTable.keys()) {
      if (this._processQueueTable.get(key)) {
        const messageQueue = this._processQueueTable.get(key).messageQueue;
        const brokerName = messageQueue.brokerName;
        let mqs = [];
        if (brokerMqs.has(brokerName)) {
          mqs = brokerMqs.get(brokerName);
        }
        mqs.push(messageQueue);
        brokerMqs.set(brokerName, mqs);
      }
    }
    return brokerMqs;
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

  async lock(messageQueue) {
    const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(messageQueue.brokerName, MixAll.MASTER_ID, true);
    if (!findBrokerResult) {
      return;
    }
    try {
      const mqs = [ messageQueue ];
      const lockedMq = await this._mqClient.lockBatchMQ(findBrokerResult.brokerAddr, this.consumerGroup, this._mqClient.clientId, mqs, 1000);
      for (const messageQueue of lockedMq) {
        const item = this._processQueueTable.get(messageQueue.key);
        const processQueue = item && item.processQueue;
        if (processQueue) {
          processQueue.locked = true;
          processQueue.lastLockTimestamp = Date.now();
        }
      }
      const lockOK = lockedMq.some(mq => mq.key === messageQueue.key);
      this.logger.info('the message queue lock %s, %s %s', lockOK ? 'OK' : 'Failed', this.consumerGroup, messageQueue.key);
      return lockOK;
    } catch (e) {
      this.logger.error('lockBatchMQ exception, %s', messageQueue.key);
    }
  }

  async unlock(messageQueue, delay) {
    if (delay >= 0) {
      await this._sleep(delay);
    }
    const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(messageQueue.brokerName, MixAll.MASTER_ID, true);
    if (!findBrokerResult) {
      return;
    }
    try {
      const mqs = [ messageQueue ];
      await this._mqClient.unlockBatchMQ(findBrokerResult.brokerAddr, this.consumerGroup, this._mqClient.clientId, mqs, 1000);
      this.logger.log('lockBatchMQ ok, group %s, mq %s', this.consumerGroup, messageQueue.key);
    } catch (e) {
      this.logger.error('lockBatchMQ exception, %s', messageQueue.key);
    }
  }

  /* eslint-disable no-loop-func */
  async lockAll() {
    const brokerMqs = this.buildProcessQueueTableByBrokerName();
    const lockTasks = [];
    for (const key of brokerMqs.keys()) {
      const mqs = brokerMqs.get(key);
      if (mqs && mqs.length >= 0) {
        const brokerName = key;
        const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
        if (!findBrokerResult) {
          continue;
        }
        lockTasks.push(
          (async () => {
            try {
              const lockOKMQSet = await this._mqClient.lockBatchMQ(findBrokerResult.brokerAddr, this.consumerGroup, this._mqClient.clientId, mqs, 1000);
              lockOKMQSet.forEach(mq => {
                const item = this._processQueueTable.get(mq.key);
                const processQueue = item && item.processQueue;
                if (processQueue != null) {
                  if (!processQueue.locked) {
                    this.logger.info('the message queue locked OK, Group: %s %s', this.consumerGroup, mq.key);
                  }
                  processQueue.locked = true;
                  processQueue.lastLockTimestamp = Date.now();
                }
              });
              for (const messageQueue of mqs) {
                if (!lockOKMQSet.some(lockOKMQ => lockOKMQ.key === messageQueue.key)) {
                  const item = this._processQueueTable.get(messageQueue.key);
                  const processQueue = item && item.processQueue;
                  if (processQueue) {
                    processQueue.locked = false;
                    this.logger.warn('the message queue locked Failed, Group: %s %s', this.consumerGroup, messageQueue.key);
                  }
                }
              }
            } catch (e) {
              this.logger.error('lockBatchMQ exception, %s', brokerName);
            }
          })()
        );
      }
    }
    // wait until all of (success or fail)
    await Promise.all(lockTasks);
    return;
  }

  async unlockAll() {
    const brokerMqs = this.buildProcessQueueTableByBrokerName();
    const unlockTasks = [];
    for (const key of brokerMqs.keys()) {
      const mqs = brokerMqs.get(key);
      if (mqs && mqs.length >= 0) {
        const brokerName = key;
        const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
        if (!findBrokerResult) {
          continue;
        }
        unlockTasks.push(
          this._mqClient.unlockBatchMQ(findBrokerResult.brokerAddr, this.consumerGroup, this._mqClient.clientId, mqs, 1000)
            .catch(() => this.logger.error('unlockBatchMQ exception, %s', brokerName))
        );
      }
    }
    // wait until all of (success or fail)
    await Promise.all(unlockTasks);
    return;
  }

  async tryLockLaterAndReconsume(messageQueue, processQueue, delayMills, args) {
    await this._sleep(delayMills);

    const lockOK = await this.lock(messageQueue);
    if (lockOK) {
      this.submitConsumeRequestLater(messageQueue, processQueue, 10, args);
    } else {
      this.submitConsumeRequestLater(messageQueue, processQueue, 3000, args);
    }
  }

  async submitConsumeRequestLater(messageQueue, processQueue, suspendTimeMillis, args) {
    let timeMillis = suspendTimeMillis;
    if (timeMillis === -1) {
      timeMillis = this.options.suspendCurrentQueueTimeMillis;
    }

    if (timeMillis < 10) {
      timeMillis = 10;
    } else if (timeMillis > 30000) {
      timeMillis = 30000;
    }

    await this._sleep(timeMillis);
    this.submitConsumeRequest(messageQueue, processQueue, false, args);
  }

  // orderlyProcessChain里存有处理顺序消息的Promise链, 避免内存泄漏的嵌套写法
  submitConsumeRequest(messageQueue, processQueue, checkDispathToConsume, args) {
    if (checkDispathToConsume && processQueue.consuming === true || !processQueue.msgCount) {
      return;
    }
    const prevTask = Promise.resolve(processQueue.orderlyProcessChain);
    const task = new Promise(async resolve => {
      try {
        await prevTask;
      } catch (e) {
        this.logger.error('submitConsumeRequest task exception', e);
      }

      resolve(this._consumeMessageQueueOrderly(messageQueue, args));
    });
    processQueue.orderlyProcessChain = task;
  }

  /**
   * subscribe
   * @param {String} topic - topic
   * @param {String} subExpression - tag
   * @param {Function} handler - message handler
   * @return {void}
   */
  subscribe(topic, subExpression, handler) {
    // 添加 namespace 前缀
    topic = this.formatTopic(topic);

    if (arguments.length === 2) {
      handler = subExpression;
      subExpression = null;
    }
    assert(is.asyncFunction(handler), '[MQPushConsumer] handler should be a asyncFunction');
    assert(!this.subscriptions.has(topic), `[MQPushConsumer] ONLY one handler allowed for topic=${topic}`);

    const subscriptionData = this.buildSubscriptionData(this.consumerGroup, topic, subExpression);
    const tagsSet = subscriptionData.tagsSet;
    const needFilter = !!tagsSet.length;
    const args = { needFilter, tagsSet, subExpression };
    this.subscriptions.set(topic, {
      handler,
      subscriptionData,
    });

    (async () => {
      try {
        await this.ready();
        // 如果 topic 没有路由信息，先更新一下
        if (!this._topicSubscribeInfoTable.has(topic)) {
          await this._mqClient.updateAllTopicRouterInfo();
          await this._mqClient.sendHeartbeatToAllBroker();
          await this._mqClient.doRebalance();
        }

        // 消息消费循环
        if (this.consumeOrderly) {
          this.on('mq_changed', mqKey => {
            if (this._isClosed) {
              this.removeListener('mq_changed');
            }
            const item = this._processQueueTable.get(mqKey);
            const { messageQueue, processQueue } = item;
            this.submitConsumeRequest(messageQueue, processQueue, true, args);
          });
        } else {
          while (!this._isClosed && this.subscriptions.has(topic)) {
            const hasMsg = await this._consumeMessage(topic, needFilter, tagsSet, subExpression);
            if (!hasMsg) {
              await this.await(`topic_${topic}_changed`);
            }
          }
        }

      } catch (err) {
        this._handleError(err);
      }
    })();
  }

  async _consumeMessageQueueOrderly(mq, args) {
    const { needFilter, tagsSet, subExpression } = args;
    const item = this._processQueueTable.get(mq.key);
    if (item) {
      const pq = item.processQueue;
      pq.consuming = true;
      if (this.messageModel === MessageModel.BROADCASTING || (pq.locked && !pq.lockExpired)) {
        let continueConsume = true;
        const beginTime = Date.now();
        while (continueConsume) {
          if (pq.droped) {
            this.logger.warn("the message queue not be able to consume, because it's dropped. %s", mq.key);
            break;
          }
          if (this.messageModel === MessageModel.CLUSTERING) {
            if (!pq.locked) {
              this.logger.warn('the message queue not locked, so consume later, %s', mq.key);
              this.tryLockLaterAndReconsume(mq, pq, 10, args);
              break;
            }
            if (pq.lockExpired) {
              this.logger.warn('the message queue lock expired, so consume later, %s', mq.key);
              this.tryLockLaterAndReconsume(mq, pq, 10, args);
              break;
            }
          }
          const interval = Date.now() - beginTime;
          if (interval > 60000) {
            this.submitConsumeRequestLater(mq, pq, 10, args);
            break;
          }
          try {
            let msgs;
            if (this.parallelConsumeLimit > pq.msgCount) {
              msgs = pq.msgList.slice(0, pq.msgCount);
            } else {
              msgs = pq.msgList.slice(0, this.parallelConsumeLimit);
            }
            for (const msg of msgs) {
              const handler = this._subscriptions.get(msg.topic).handler;
              if (!msg.tags || !needFilter || tagsSet.includes(msg.tags)) {
                // 此处不可能失败
                await this.consumeSingleMsg(handler, msg, mq, pq, true);
              } else {
                this.logger.debug('[MQPushConsumer] message filter by tags=%s, msg.tags=%s', subExpression, msg.tags);
              }
            }
            // 注意这里必须是批量确认
            const offset = pq.remove(msgs.length);
            if (offset >= 0) {
              this._offsetStore.updateOffset(mq, offset, true);
            }
          } catch (e) {
            continue;
          }

          if (!pq.msgCount) {
            continueConsume = false;
          }
        }
      } else {
        if (pq.droped) {
          this.logger.warn("the message queue not be able to consume, because it's dropped. %s", mq.key);
        } else {
          this.tryLockLaterAndReconsume(mq, pq, 100, args);
        }
      }
      pq.consuming = false;
    }
    return;
  }

  async _consumeMessage(topic, needFilter, tagsSet, subExpression) {
    const mqList = this._topicSubscribeInfoTable.get(topic);
    let hasMsg = false;
    if (mqList && mqList.length) {
      for (const mq of mqList) {
        const item = this._processQueueTable.get(mq.key);
        if (item) {
          const pq = item.processQueue;
          while (pq.msgCount) {
            hasMsg = true;
            let msgs;
            if (this.parallelConsumeLimit > pq.msgCount) {
              msgs = pq.msgList.slice(0, pq.msgCount);
            } else {
              msgs = pq.msgList.slice(0, this.parallelConsumeLimit);
            }
            // 并发消费任务
            const consumeTasks = [];
            for (const msg of msgs) {
              const handler = this._subscriptions.get(msg.topic).handler;
              if (!msg.tags || !needFilter || tagsSet.includes(msg.tags)) {
                consumeTasks.push(this.consumeSingleMsg(handler, msg, mq, pq, false));
              } else {
                this.logger.debug('[MQPushConsumer] message filter by tags=%s, msg.tags=%s', subExpression, msg.tags);
              }
            }
            // 必须全部成功
            try {
              await Promise.all(consumeTasks);
            } catch (err) {
              continue;
            }
            // 注意这里必须是批量确认
            const offset = pq.remove(msgs.length);
            if (offset >= 0) {
              this._offsetStore.updateOffset(mq, offset, true);
            }
          }
        }
      }
    }

    return hasMsg;
  }

  async consumeSingleMsg(handler, msg, mq, pq, isOrder) {
    // 集群消费模式下，如果消费失败，反复重试
    while (!this._isClosed) {
      if (msg.reconsumeTimes > this.options.maxReconsumeTimes) {
        this.logger.warn('[MQPushConsumer] consume message failed, drop it for reconsumeTimes=%d and maxReconsumeTimes=%d, msgId: %s, originMsgId: %s',
          msg.reconsumeTimes, this.options.maxReconsumeTimes, msg.msgId, msg.originMessageId);
        return;
      }

      utils.resetRetryTopic(msg, this.consumerGroup);

      try {
        await handler(msg, mq, pq);
        return;
      } catch (err) {
        err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
        this.emit('error', err);
        if (isOrder) {
          if (msg.reconsumeTimes >= this.options.maxReconsumeTimes) {
            try {
              // 投入死信队列
              await this.sendMessageBackFailed(msg, this.consumerGroup, true);
              return;
            } catch (err) {
              this.emit('error', err);
              this.logger.error('[MQPushConsumer] send dead message failed, continue local retry, msgId: %s', msg.msgId);
              // 投入死信队列失败
            }
          }
          await this._sleep(5000);
          msg.reconsumeTimes++;
        } else {
          if (this.messageModel === MessageModel.CLUSTERING) {
            // 发送重试消息
            try {
              // delayLevel 为 0 代表由服务端控制重试间隔
              await this.sendMessageBack(msg, 0, mq.brokerName, this.consumerGroup);
              return;
            } catch (err) {
              this.emit('error', err);
              this.logger.error('[MQPushConsumer] send reconsume message failed, fallback to local retry, msgId: %s', msg.msgId);
              // 重试消息发送失败，本地重试
              await this._sleep(5000);
            }
            // 本地重试情况下需要给 reconsumeTimes +1
            msg.reconsumeTimes++;
          } else {
            this.logger.warn('[MQPushConsumer] BROADCASTING consume message failed, drop it, msgId: %s', msg.msgId);
            return;
          }
        }
      }
    }
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

    if (this.consumeOrderly) {
      if (processQueue.locked) {
        if (pullRequest.lockedFirst !== true) {
          const offset = await this.computePullFromWhere(pullRequest.messageQueue);
          const brokerBusy = offset < pullRequest.nextOffset;
          this.logger.info('the first time to pull message, so fix offset from broker. pullRequest: %j NewOffset: %s brokerBusy: %s',
            pullRequest, offset, brokerBusy);
          if (brokerBusy) {
            this.logger.info('the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: %j NewOffset: %s',
              pullRequest, offset);
          }
          pullRequest.lockedFirst = true;
          pullRequest.nextOffset = offset;
        }
      } else {
        await this._sleep(this.options.pullTimeDelayMillsWhenException);
        this.logger.info('pull message later because not locked in broker, %s', pullRequest.messageQueue.key);
        return;
      }
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
        this.logger.info('[MQPushConsumer] pull message success, found new message size: %d, topic: %s, consumerGroup: %s, messageQueue: %s cost: %dms.',
          pullResult.msgFoundList.length, messageQueue.topic, this.consumerGroup, messageQueue.key, pullRT);

        // submit to consumer
        processQueue.putMessage(pullResult.msgFoundList);
        this.emit(`topic_${messageQueue.topic}_changed`);
        this.emit('mq_changed', messageQueue.key);
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
    // 仅当已拉下的消息消费完的情况下才更新 offset
    if (pullRequest.processQueue.msgCount === 0) {
      this._offsetStore.updateOffset(pullRequest.messageQueue, pullRequest.nextOffset, true);
    }
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
    const isOrder = this.consumeOrderly;
    for (const topic of this.subscriptions.keys()) {
      await this.rebalanceByTopic(topic, isOrder);
    }
  }

  async rebalanceByTopic(topic, isOrder) {
    this.logger.info('[mq:consumer] rebalanceByTopic: %s, messageModel: %s', topic, this.messageModel);
    const mqSet = this._topicSubscribeInfoTable.get(topic); // messageQueue list
    if (!mqSet || !mqSet.length) {
      this.logger.warn('[mq:consumer] doRebalance, %s, but the topic[%s] not exist.', this.consumerGroup, topic);
      return;
    }

    let changed;
    let allocateResult = mqSet;
    if (this.options.isBroadcast) {
      changed = await this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
    } else {
      const cidAll = await this._mqClient.findConsumerIdList(topic, this.consumerGroup);
      this.logger.info('[mq:consumer] rebalance topic: %s, with consumer ids: %j', topic, cidAll);
      if (cidAll && cidAll.length) {
        // 排序
        mqSet.sort(compare);
        cidAll.sort();

        allocateResult = this.allocateMessageQueueStrategy.allocate(this.consumerGroup, this._mqClient.clientId, mqSet, cidAll);
        this.logger.info('[mq:consumer] allocate queue for group: %s, clientId: %s, result: %j', this.consumerGroup, this._mqClient.clientId, allocateResult);
        changed = await this.updateProcessQueueTableInRebalance(topic, allocateResult, isOrder);
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
   * @param {Boolean} isOrder - is order or not
   * @return {void}
   */
  async updateProcessQueueTableInRebalance(topic, mqSet, isOrder) {
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

      if (isOrder && !(await this.lock(messageQueue))) {
        this.logger.warn('doRebalance, %s, add a new mq failed, %s, because lock failed', this.consumerGroup, messageQueue.key);
        continue;
      }

      const nextOffset = await this.computePullFromWhere(messageQueue);
      // double check
      if (this._processQueueTable.has(messageQueue.key)) {
        continue;
      }

      if (nextOffset >= 0) {
        const processQueue = new ProcessQueue();
        if (isOrder) {
          // this pq has been locked
          processQueue.locked = true;
          processQueue.lastLockTimestamp = Date.now();
        }
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
    if (this.consumeOrderly && this.messageModel === MessageModel.CLUSTERING) {
      try {
        // 异步unlock
        this.unlock(messageQueue, 20000);
      } catch (e) {
        this.logger.error('removeUnnecessaryMessageQueue Exception');
      }
    }
  }

  async sendMessageBack(msg, delayLevel, brokerName, consumerGroup) {
    const brokerAddr = brokerName ? this._mqClient.findBrokerAddressInPublish(brokerName) :
      msg.storeHost;
    const thatConsumerGroup = consumerGroup ? consumerGroup : this.consumerGroup;
    try {
      await this._mqClient.consumerSendMessageBack(
        brokerAddr,
        msg,
        thatConsumerGroup,
        delayLevel,
        3000,
        this.options.maxReconsumeTimes);
    } catch (err) {
      err.mesasge = 'sendMessageBack() occurred an exception, ' + thatConsumerGroup + ', ' + err.mesasge;
      this._handleError(err);

      let newMsg;
      if (MixAll.isRetryTopic(msg.topic)) {
        newMsg = msg;
      } else {
        newMsg = new Message(MixAll.getRetryTopic(thatConsumerGroup), '', msg.body);
        newMsg.flag = msg.flag;
        newMsg.properties = msg.properties;
        newMsg.originMessageId = msg.originMessageId || msg.msgId;
        newMsg.retryTopic = msg.topic;
        // 这里需要加 1，因为如果 maxReconsumeTimes 为 1，那么这条 retry 消息发出去始终不会被重新投递了
        newMsg.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] = String(this.options.maxReconsumeTimes + 1);
      }

      newMsg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = String(msg.reconsumeTimes + 1);
      newMsg.delayTimeLevel = 0;
      await (await MQProducer.getDefaultProducer()).send(newMsg);
    }
  }

  // 投入死信队列
  async sendMessageBackFailed(msg, consumerGroup, orderly) {
    let newMsg;
    const thatConsumerGroup = consumerGroup ? consumerGroup : this.consumerGroup;
    if (!orderly && MixAll.isRetryTopic(msg.topic)) {
      newMsg = msg;
    } else {
      newMsg = new Message(MixAll.getRetryTopic(thatConsumerGroup), '', msg.body);
      newMsg.flag = msg.flag;
      newMsg.properties = msg.properties;
      newMsg.originMessageId = msg.originMessageId || msg.msgId;
      newMsg.retryTopic = msg.topic;
      newMsg.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] = this.options.maxReconsumeTimes;
    }

    newMsg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = String(orderly ? msg.reconsumeTimes : msg.reconsumeTimes + 1);
    newMsg.delayTimeLevel = 3 + msg.reconsumeTimes;
    await (await MQProducer.getDefaultProducer()).send(newMsg);
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
