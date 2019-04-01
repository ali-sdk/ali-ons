/* istanbul ignore next */
/* eslint valid-jsdoc:0 */
'use strict';

class AllocateMessageQueueStrategy {
  /**
   * 给当前的 ConsumerId 分配队列
   * @param {String} consumerGroup -
   * @param {String} currentCID - 当前 ConsumerId
   * @param {Array} mqAll - 当前 Topic 的所有队列集合，无重复数据，且有序
   * @param {Array} cidAll - 当前订阅组的所有 Consumer 集合，无重复数据，且有序
   */
  allocate(consumerGroup, currentCID, mqAll, cidAll) { /* eslint no-unused-vars: 0 */
    throw new Error('no implementation');
  }

  /**
   * rebalance 算法的名字
   */
  get name() {
    throw new Error('no implementation');
  }

}

module.exports = AllocateMessageQueueStrategy;
