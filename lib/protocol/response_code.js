'use strict';

// 成功
exports.SUCCESS = 0;
// 发生了未捕获异常
exports.SYSTEM_ERROR = 1;
// 由于线程池拥堵，系统繁忙
exports.SYSTEM_BUSY = 2;
// 请求代码不支持
exports.REQUEST_CODE_NOT_SUPPORTED = 3;

// Broker 刷盘超时
exports.FLUSH_DISK_TIMEOUT = 10;
// Broker 同步双写，Slave不可用
exports.SLAVE_NOT_AVAILABLE = 11;
// Broker 同步双写，等待Slave应答超时
exports.FLUSH_SLAVE_TIMEOUT = 12;
// Broker 消息非法
exports.MESSAGE_ILLEGAL = 13;
// Broker, Namesrv 服务不可用，可能是正在关闭或者权限问题
exports.SERVICE_NOT_AVAILABLE = 14;
// Broker, Namesrv 版本号不支持
exports.VERSION_NOT_SUPPORTED = 15;
// Broker, Namesrv 无权限执行此操作，可能是发、收、或者其他操作
exports.NO_PERMISSION = 16;
// Broker, Topic不存在
exports.TOPIC_NOT_EXIST = 17;
// Broker, Topic已经存在，创建Topic
exports.TOPIC_EXIST_ALREADY = 18;
// Broker 拉消息未找到（请求的Offset等于最大Offset，最大Offset无对应消息）
exports.PULL_NOT_FOUND = 19;
// Broker 可能被过滤，或者误通知等
exports.PULL_RETRY_IMMEDIATELY = 20;
// Broker 拉消息请求的Offset不合法，太小或太大
exports.PULL_OFFSET_MOVED = 21;
// Broker 查询消息未找到
exports.QUERY_NOT_FOUND = 22;
// Broker 订阅关系解析失败
exports.SUBSCRIPTION_PARSE_FAILED = 23;
// Broker 订阅关系不存在
exports.SUBSCRIPTION_NOT_EXIST = 24;
// Broker 订阅关系不是最新的
exports.SUBSCRIPTION_NOT_LATEST = 25;
// Broker 订阅组不存在
exports.SUBSCRIPTION_GROUP_NOT_EXIST = 26;
// Producer 事务应该被提交
exports.TRANSACTION_SHOULD_COMMIT = 200;
// Producer 事务应该被回滚
exports.TRANSACTION_SHOULD_ROLLBACK = 201;
// Producer 事务状态未知
exports.TRANSACTION_STATE_UNKNOW = 202;
// Producer ProducerGroup错误
exports.TRANSACTION_STATE_GROUP_WRONG = 203;
// 单元化消息，需要设置 buyerId
exports.NO_BUYER_ID = 204;

// 单元化消息，非本单元消息
exports.NOT_IN_CURRENT_UNIT = 205;

// Consumer不在线
exports.CONSUMER_NOT_ONLINE = 206;

// Consumer消费消息超时
exports.CONSUME_MSG_TIMEOUT = 207;
