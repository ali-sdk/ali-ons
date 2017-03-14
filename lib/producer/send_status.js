'use strict';

module.exports = {
  // 消息发送成功
  SEND_OK: 'SEND_OK',
  // 消息发送成功，但是服务器刷盘超时，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失
  FLUSH_DISK_TIMEOUT: 'FLUSH_DISK_TIMEOUT',
  // 消息发送成功，但是服务器同步到Slave时超时，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失
  FLUSH_SLAVE_TIMEOUT: 'FLUSH_SLAVE_TIMEOUT',
  // 消息发送成功，但是此时slave不可用，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失
  SLAVE_NOT_AVAILABLE: 'SLAVE_NOT_AVAILABLE',
};
