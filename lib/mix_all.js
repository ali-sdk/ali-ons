'use strict';

exports.DEFAULT_TOPIC = 'TBW102';
exports.DEFAULT_PRODUCER_GROUP = 'DEFAULT_PRODUCER';
exports.DEFAULT_CONSUMER_GROUP = 'DEFAULT_CONSUMER';

exports.DEFAULT_CHARSET = 'UTF-8';
exports.MASTER_ID = 0;
// 为每个Consumer Group建立一个默认的Topic，前缀 + GroupName，用来保存处理失败需要重试的消息
exports.RETRY_GROUP_TOPIC_PREFIX = '%RETRY%';
// 为每个Consumer Group建立一个默认的Topic，前缀 + GroupName，用来保存重试多次都失败，接下来不再重试的消息
exports.DLQ_GROUP_TOPIC_PREFIX = '%DLQ%';

exports.getRetryTopic = consumerGroup => {
  return exports.RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
};
