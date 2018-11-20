'use strict';

const env = process.env;

// export ALI_SDK_ONS_ID=your-accesskey
// export ALI_SDK_ONS_SECRET=your-secretkey

module.exports = {
  accessKeyId: env.ALI_SDK_ONS_ID,
  accessKeySecret: env.ALI_SDK_ONS_SECRET,
  producerGroup: 'PID_GXCSOCCER',
  consumerGroup: 'CID_GXCSOCCER',
  topic: 'GXCSOCCER',
  onsAddr: 'http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet',
};
