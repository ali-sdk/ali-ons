'use strict';

class AllocateMessageQueueStatically {
  get name() {
    return 'STATIC';
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
      return result;
    }

    result.push(mqAll[0]);
    return result;
  }
}

module.exports = AllocateMessageQueueStatically;
