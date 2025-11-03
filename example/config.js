'use strict';

const env = process.env;

// export ALI_SDK_ONS_ID=your-accesskey
// export ALI_SDK_ONS_SECRET=your-secretkey

module.exports = {
  accessKeyId: env.ALI_SDK_ONS_ID,
  accessKeySecret: env.ALI_SDK_ONS_SECRET,
  producerGroup: 'PID_GXCSOCCER',
  consumerGroup: 'GID_alions',
  topic: 'TP_alions_test_topic',
  // https://help.aliyun.com/document_detail/102895.html 阿里云产品更新，支持实例化
  nameSrv: 'localhost:9876',
  onsAddr: 'localhost:10911',
};
