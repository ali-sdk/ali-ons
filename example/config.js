'use strict';

const env = process.env;

// export ALI_SDK_ONS_ID=your-accesskey
// export ALI_SDK_ONS_SECRET=your-secretkey
// export ALI_SDK_ONS_CGROUP=your-consumer-group
// export ALI_SDK_ONS_PGROUP=your-producer-group
// export ALI_SDK_ONS_TOPIC=your-topic

module.exports = {
  accessKey: env.ALI_SDK_ONS_ID,
  secretKey: env.ALI_SDK_ONS_SECRET,
  producerGroup: env.ALI_SDK_ONS_PGROUP,
  consumerGroup: env.ALI_SDK_ONS_CGROUP,
  topic: env.ALI_SDK_ONS_TOPIC,
  onsAddr: 'http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet',
};
