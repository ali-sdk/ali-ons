'use strict';

module.exports = {
  /**
   * 找到消息
   */
  FOUND: 'FOUND',
  /**
   * 没有新的消息可以被拉取
   */
  NO_NEW_MSG: 'NO_NEW_MSG',
  /**
   * 经过过滤后，没有匹配的消息
   */
  NO_MATCHED_MSG: 'NO_MATCHED_MSG',
  /**
   * Offset不合法，可能过大或者过小
   */
  OFFSET_ILLEGAL: 'OFFSET_ILLEGAL',
};
