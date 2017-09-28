'use strict';

const JSON2 = require('JSON2');
const bytes = require('bytes');
const fmt = require('util').format;
const ByteBuffer = require('byte');
const MessageQueue = require('./message_queue');
const RemotingClient = require('./remoting_client');
const PullStatus = require('./consumer/pull_status');
const SendStatus = require('./producer/send_status');
const RequestCode = require('./protocol/request_code');
const ResponseCode = require('./protocol/response_code');
const MessageConst = require('./message/message_const');
const MessageDecoder = require('./message/message_decoder');
const RemotingCommand = require('./protocol/command/remoting_command');

// const localIp = require('address').ip();
// const NAMESPACE_ORDER_TOPIC_CONFIG = 'ORDER_TOPIC_CONFIG';
const NAMESPACE_PROJECT_CONFIG = 'PROJECT_CONFIG';
const VIRTUAL_APPGROUP_PREFIX = '%%PROJECT_%s%%';

const byteBuffer = ByteBuffer.allocate(bytes('1m'));

/* eslint no-fallthrough:0 */

class MQClientAPI extends RemotingClient {

  /**
   * Metaq api wrapper
   * @param {Object} options
   *   - {HttpClient} urllib - http request client
   *   - {Object} [logger] - log module
   *   - {Number} [responseTimeout] - tcp response timeout
   * @constructor
   */
  constructor(options) {
    super(options);

    // virtual env project group
    this.projectGroupPrefix = null;
  }

  /**
   * start the client
   * @return {void}
   */
  * init() {
    yield super.init();
    // this.projectGroupPrefix = yield this.getProjectGroupByIp(localIp, 3000);
  }

  /**
   * get project group prefix by ip address
   * @param {String} ip - ip address
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {String} prefix
   */
  * getProjectGroupByIp(ip, timeoutMillis) {
    try {
      return yield this.getKVConfigByValue(NAMESPACE_PROJECT_CONFIG, ip, timeoutMillis);
    } catch (err) {
      err.message = `[mq:api] Can not get project config from server, ${err.message}`;
      this.logger.error(err);
      return null;
    }
  }

  /**
   * get key-value config
   * @param {String} namespace - config namespace
   * @param {String} value - config key
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {String} config value
   */
  * getKVConfigByValue(namespace, value, timeoutMillis) {
    const requestHeader = {
      namespace,
      key: value,
    };
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG_BY_VALUE, requestHeader);
    const response = yield this.invoke(null, request, timeoutMillis);
    switch (response.code) {
      case ResponseCode.SUCCESS:
      {
        const responseHeader = response.decodeCommandCustomHeader();
        return responseHeader && responseHeader.value;
      }
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * get route info of topic
   * @param {String} topic - topic
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Object} router info
   */
  * getDefaultTopicRouteInfoFromNameServer(topic, timeoutMillis) {
    const requestHeader = {
      topic,
    };

    const request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);
    const response = yield this.invoke(null, request, timeoutMillis);
    switch (response.code) {
      case ResponseCode.SUCCESS:
      {
        const body = response.body;
        if (body) {
          this.logger.info('[mq:client_api] get Topic [%s] RouteInfoFromNameServer: %s', topic, body.toString());
          // JSON.parse dose not work here
          const routerInfoData = JSON2.parse(body.toString());
          // sort
          routerInfoData.queueDatas.sort(compare);
          routerInfoData.brokerDatas.sort(compare);
          return routerInfoData;
        }
        break;
      }
      case ResponseCode.TOPIC_NOT_EXIST:
        this.logger.info('[mq:client_api] get Topic [%s] RouteInfoFromNameServer is not exist value', topic);
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * notify broker that client is offline
   * @param {String} addr - brokder address
   * @param {String} clientId - clientId
   * @param {String} producerGroup - producer group name
   * @param {String} consumerGroup - consumer group name
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {void}
   */
  * unregisterClient(addr, clientId, producerGroup, consumerGroup, timeoutMillis) {
    producerGroup = this._buildWithProjectGroup(producerGroup);
    consumerGroup = this._buildWithProjectGroup(consumerGroup);

    const requestHeader = {
      clientID: clientId,
      producerGroup,
      consumerGroup,
    };
    const request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);
    const response = yield this.invoke(addr, request, timeoutMillis);

    switch (response.code) {
      case ResponseCode.SUCCESS:
        break;
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * get route info from name server
   * @param {String} topic - topic
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Object} route info
   */
  * getTopicRouteInfoFromNameServer(topic, timeoutMillis) {
    topic = this._buildWithProjectGroup(topic);
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, {
      topic,
    });
    const response = yield this.invoke(null, request, timeoutMillis);
    switch (response.code) {
      case ResponseCode.SUCCESS:
      {
        const body = response.body;
        if (body) {
          const routerInfoData = JSON2.parse(body.toString());
          // sort
          routerInfoData.queueDatas.sort(compare);
          routerInfoData.brokerDatas.sort(compare);
          return routerInfoData;
        }
        break;
      }
      case ResponseCode.TOPIC_NOT_EXIST:
        this.logger.warn('[mq:client_api] get Topic [%s] RouteInfoFromNameServer is not exist value', topic);
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * send heartbeat
   * @param {String} addr - broker address
   * @param {Object} heartbeatData - heartbeat data
   * @param {Number} [timeout] - timeout in milliseconds
   * @return {void}
   */
  * sendHearbeat(addr, heartbeatData, timeout) {
    if (this.projectGroupPrefix) {
      for (const consumerData of heartbeatData.consumerDataSet) {
        consumerData.groupName = this._buildWithProjectGroup(consumerData.groupName);
        for (const subscriptionData of consumerData.subscriptionDataSet) {
          subscriptionData.topic = this._buildWithProjectGroup(subscriptionData.topic);
        }
      }
      for (const producerData of heartbeatData.producerDataSet) {
        producerData.groupName = this._buildWithProjectGroup(producerData.groupName);
      }
    }
    const request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
    request.body = new Buffer(JSON.stringify(heartbeatData));
    const response = yield this.invoke(addr, request, timeout);
    if (response.code !== ResponseCode.SUCCESS) {
      this._defaultHandler(request, response);
    }
  }

  /**
   * update consumer offset
   * @param {String} brokerAddr - broker address
   * @param {Object} requestHeader - request header
   * @return {void}
   */
  * updateConsumerOffsetOneway(brokerAddr, requestHeader) {
    requestHeader.consumerGroup = this._buildWithProjectGroup(requestHeader.consumerGroup);
    requestHeader.topic = this._buildWithProjectGroup(requestHeader.topic);

    const request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
    yield this.invokeOneway(brokerAddr, request);
  }

  /**
   * query consume offset
   * @param {String} brokerAddr - broker address
   * @param {Object} requestHeader - request header
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Number} offset
   */
  * queryConsumerOffset(brokerAddr, requestHeader, timeoutMillis) {
    requestHeader.consumerGroup = this._buildWithProjectGroup(requestHeader.consumerGroup);
    requestHeader.topic = this._buildWithProjectGroup(requestHeader.topic);

    const request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
    const response = yield this.invoke(brokerAddr, request, timeoutMillis);
    switch (response.code) {
      case ResponseCode.SUCCESS:
      {
        const responseHeader = response.decodeCommandCustomHeader();
        return Number(responseHeader.offset.toString());
      }
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * get current max offset of queue
   * @param {String} addr - broker address
   * @param {String} topic - topic
   * @param {Number} queueId - queue id
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Number} offset
   */
  * getMaxOffset(addr, topic, queueId, timeoutMillis) {
    topic = this._buildWithProjectGroup(topic);
    const requestHeader = {
      topic,
      queueId,
    };
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);
    const response = yield this.invoke(addr, request, timeoutMillis);

    switch (response.code) {
      case ResponseCode.SUCCESS:
      {
        const responseHeader = response.decodeCommandCustomHeader();
        // todo:
        return responseHeader && Number(responseHeader.offset);
      }
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * search consume offset by timestamp
   * @param {String} addr - broker address
   * @param {String} topic - topic
   * @param {Number} queueId - queue id
   * @param {String} timestamp - timestamp used to query
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Number} offset
   */
  * searchOffset(addr, topic, queueId, timestamp, timeoutMillis) {
    topic = this._buildWithProjectGroup(topic);
    const requestHeader = {
      topic,
      queueId,
      timestamp,
    };
    const request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
    const response = yield this.invoke(addr, request, timeoutMillis);
    switch (response.code) {
      case ResponseCode.SUCCESS:
      {
        const responseHeader = response.decodeCommandCustomHeader();
        // todo:
        return responseHeader && Number(responseHeader.offset);
      }
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * get all consumer's id in same group
   * @param {String} addr - broker address
   * @param {String} consumerGroup - consumer group
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Array} consumer list
   */
  * getConsumerIdListByGroup(addr, consumerGroup, timeoutMillis) {
    consumerGroup = this._buildWithProjectGroup(consumerGroup);
    const requestHeader = {
      consumerGroup,
    };
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
    const response = yield this.invoke(addr, request, timeoutMillis);
    switch (response.code) {
      case ResponseCode.SUCCESS:
        if (response.body) {
          const body = JSON2.parse(response.body.toString());
          return body.consumerIdList;
        }
        break;
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * pull message from broker
   * @param {String} brokerAddr - broker address
   * @param {Object} requestHeader - request header
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Object} pull result
   */
  * pullMessage(brokerAddr, requestHeader, timeoutMillis) {
    requestHeader.consumerGroup = this._buildWithProjectGroup(requestHeader.consumerGroup);
    requestHeader.topic = this._buildWithProjectGroup(requestHeader.topic);

    const request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
    const response = yield this.invoke(brokerAddr, request, timeoutMillis);
    let pullStatus = PullStatus.NO_NEW_MSG;
    switch (response.code) {
      case ResponseCode.SUCCESS:
        pullStatus = PullStatus.FOUND;
        break;
      case ResponseCode.PULL_NOT_FOUND:
        pullStatus = PullStatus.NO_NEW_MSG;
        break;
      case ResponseCode.PULL_RETRY_IMMEDIATELY:
        pullStatus = PullStatus.NO_MATCHED_MSG;
        break;
      case ResponseCode.PULL_OFFSET_MOVED:
        pullStatus = PullStatus.OFFSET_ILLEGAL;
        break;
      default:
        this._defaultHandler(request, response);
        break;
    }

    const responseHeader = response.decodeCommandCustomHeader();
    let msgList = [];
    if (pullStatus === PullStatus.FOUND) {
      byteBuffer.reset();
      byteBuffer.put(response.body).flip();
      msgList = MessageDecoder.decodes(byteBuffer);

      for (const msg of msgList) {
        msg.topic = this._clearProjectGroup(msg.topic);
        msg.properties[MessageConst.PROPERTY_MIN_OFFSET] = responseHeader.minOffset.toString();
        msg.properties[MessageConst.PROPERTY_MAX_OFFSET] = responseHeader.maxOffset.toString();
      }
    }

    return {
      pullStatus,
      nextBeginOffset: Number(responseHeader.nextBeginOffset),
      minOffset: Number(responseHeader.minOffset),
      maxOffset: Number(responseHeader.maxOffset),
      msgFoundList: msgList,
      suggestWhichBrokerId: responseHeader.suggestWhichBrokerId,
    };
  }

  /**
   * create topic
   * @param {String} addr - broker address
   * @param {String} defaultTopic - default topic: TBW102
   * @param {Object} topicConfig - new topic config
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {void}
   */
  * createTopic(addr, defaultTopic, topicConfig, timeoutMillis) {
    const topicWithProjectGroup = this._buildWithProjectGroup(topicConfig.topicName);
    const requestHeader = {
      topic: topicWithProjectGroup,
      defaultTopic,
      readQueueNums: topicConfig.readQueueNums,
      writeQueueNums: topicConfig.writeQueueNums,
      perm: topicConfig.perm,
      topicFilterType: topicConfig.topicFilterType,
      topicSysFlag: topicConfig.topicSysFlag,
      order: topicConfig.order,
    };
    const request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
    const response = yield this.invoke(addr, request, timeoutMillis);
    switch (response.code) {
      case ResponseCode.SUCCESS:
        return;
      default:
        this._defaultHandler(request, response);
        break;
    }
  }

  /**
   * send message
   * @param {String} brokerAddr - broker address
   * @param {String} brokerName - broker name
   * @param {Message} msg - msg object
   * @param {Object} requestHeader - request header
   * @param {Number} [timeoutMillis] - timeout in milliseconds
   * @return {Object} sendResult
   */
  * sendMessage(brokerAddr, brokerName, msg, requestHeader, timeoutMillis) {
    msg.topic = this._buildWithProjectGroup(msg.topic);
    requestHeader.producerGroup = this._buildWithProjectGroup(requestHeader.producerGroup);
    requestHeader.topic = this._buildWithProjectGroup(requestHeader.topic);

    const requestHeaderV2 = {
      a: requestHeader.producerGroup,
      b: requestHeader.topic,
      c: requestHeader.defaultTopic,
      d: requestHeader.defaultTopicQueueNums,
      e: requestHeader.queueId,
      f: requestHeader.sysFlag,
      g: requestHeader.bornTimestamp,
      h: requestHeader.flag,
      i: requestHeader.properties,
      j: requestHeader.reconsumeTimes,
      k: requestHeader.unitMode,
    };
    const request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
    request.body = msg.body;
    const response = yield this.invoke(brokerAddr, request, timeoutMillis);
    let sendStatus = SendStatus.SEND_OK;
    switch (response.code) {
      case ResponseCode.FLUSH_DISK_TIMEOUT:
        sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
        break;
      case ResponseCode.FLUSH_SLAVE_TIMEOUT:
        sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
        break;
      case ResponseCode.SLAVE_NOT_AVAILABLE:
        sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
        break;
      case ResponseCode.SUCCESS:
        sendStatus = SendStatus.SEND_OK;
        break;
      default:
        this._defaultHandler(request, response);
        break;
    }
    const responseHeader = response.decodeCommandCustomHeader();
    const messageQueue = new MessageQueue(msg.topic, brokerName, responseHeader.queueId);
    messageQueue.topic = this._clearProjectGroup(messageQueue.topic);

    return {
      sendStatus,
      msgId: responseHeader.msgId,
      messageQueue,
      queueOffset: responseHeader.queueOffset,
    };
  }

  // * viewMessage(brokerAddr, phyoffset, timeoutMillis) {
  //   const requestHeader = {
  //     offset: phyoffset,
  //   };
  //   const request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);
  //   const response = yield this.invoke(brokerAddr, request, timeoutMillis);
  //   switch (response.code) {
  //     case ResponseCode.SUCCESS:
  //       {
  //         const byteBuffer = ByteBuffer.wrap(response.body);
  //         const messageExt = MessageDecoder.decode(byteBuffer);
  //         // 清除虚拟运行环境相关的projectGroupPrefix
  //         if (this.projectGroupPrefix) {
  //           messageExt.topic = this._clearProjectGroup(messageExt.topic, this.projectGroupPrefix);
  //         }
  //         return messageExt;
  //       }
  //     default:
  //       this._defaultHandler(request, response);
  //       break;
  //   }
  // }

  // default handler
  _defaultHandler(request, response) {
    const err = new Error(response.remark);
    err.name = 'MQClientException';
    err.code = response.code;
    throw err;
  }

  _buildWithProjectGroup(origin) {
    if (this.projectGroupPrefix) {
      const prefix = fmt(VIRTUAL_APPGROUP_PREFIX, this.projectGroupPrefix);
      if (!origin.endsWith(prefix)) {
        return origin + prefix;
      }
      return origin;
    }
    return origin;
  }

  _clearProjectGroup(origin) {
    const prefix = fmt(VIRTUAL_APPGROUP_PREFIX, this.projectGroupPrefix);
    if (prefix && origin.endsWith(prefix)) {
      return origin.slice(0, origin.lastIndexOf(prefix));
    }
    return origin;
  }
}

module.exports = MQClientAPI;

// Helper
// ---------------
function compare(routerA, routerB) {
  if (routerA.brokerName > routerB.brokerName) {
    return 1;
  } else if (routerA.brokerName < routerB.brokerName) {
    return -1;
  }
  return 0;
}
