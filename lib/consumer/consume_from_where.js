'use strict';

module.exports = {
  /**
   * 一个新的订阅组第一次启动从队列的最后位置开始消费<br>
   * 后续再启动接着上次消费的进度开始消费
   */
  CONSUME_FROM_LAST_OFFSET: 'CONSUME_FROM_LAST_OFFSET',

  CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST: 'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST',
  CONSUME_FROM_MIN_OFFSET: 'CONSUME_FROM_MIN_OFFSET',
  CONSUME_FROM_MAX_OFFSET: 'CONSUME_FROM_MAX_OFFSET',
  /**
   * 一个新的订阅组第一次启动从队列的最前位置开始消费<br>
   * 后续再启动接着上次消费的进度开始消费
   */
  CONSUME_FROM_FIRST_OFFSET: 'CONSUME_FROM_FIRST_OFFSET',
  /**
   * 一个新的订阅组第一次启动从指定时间点开始消费<br>
   * 后续再启动接着上次消费的进度开始消费<br>
   * 时间点设置参见DefaultMQPushConsumer.consumeTimestamp参数
   */
  CONSUME_FROM_TIMESTAMP: 'CONSUME_FROM_TIMESTAMP',
};
