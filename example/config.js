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
  // https://help.aliyun.com/document_detail/102895.html 阿里云产品更新，支持实例化
  // nameSrv: '112.124.141.191:80',
  onsAddr: 'http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet',
};
