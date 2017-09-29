'use strict';

const debug = require('debug')('mq:allocate');
const AllocateMessageQueueStrategy = require('./allocate_message_queue_strategy');

class AllocateMessageQueueAveragely extends AllocateMessageQueueStrategy {
  get name() {
    return 'AVG';
  }

  allocate(consumerGroup, currentCID, mqAll, cidAll) {
    if (!currentCID || currentCID.length < 1) {
      throw new Error('currentCID is empty');
    }
    if (!mqAll || !mqAll.length) {
      throw new Error('mqAll is null or mqAll empty');
    }
    if (!cidAll || !cidAll.length) {
      throw new Error('cidAll is null or cidAll empty');
    }

    const result = [];
    const index = cidAll.indexOf(currentCID);
    if (index === -1) { // 不存在此ConsumerId ,直接返回
      debug('[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %j', //
        consumerGroup, //
        currentCID, //
        cidAll);
      return result;
    }

    const mqLen = mqAll.length;
    const cidLen = cidAll.length;
    const mod = mqLen % cidLen;
    let averageSize = mqLen <= cidLen ?
      1 :
      mod > 0 && index < mod ?
        mqLen / cidLen + 1 :
        mqLen / cidLen;
    averageSize = Math.floor(averageSize); // 取整
    const startIndex = mod > 0 && index < mod ? index * averageSize : index * averageSize + mod;
    const range = Math.min(averageSize, mqLen - startIndex);
    for (let i = 0; i < range; i++) {
      result.push(mqAll[(startIndex + i) % mqLen]);
    }
    return result;
  }
}

module.exports = AllocateMessageQueueAveragely;
