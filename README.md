ali-ons
=======

[![NPM version][npm-image]][npm-url]
[![build status][travis-image]][travis-url]
[![David deps][david-image]][david-url]

[npm-image]: https://img.shields.io/npm/v/ali-ons.svg?style=flat-square
[npm-url]: https://npmjs.org/package/ali-ons
[travis-image]: https://img.shields.io/travis/ali-sdk/ali-ons.svg?style=flat-square
[travis-url]: https://travis-ci.org/ali-sdk/ali-ons
[david-image]: https://img.shields.io/david/ali-sdk/ali-ons.svg?style=flat-square
[david-url]: https://david-dm.org/ali-sdk/ali-ons

Aliyun Open Notification Service Client (base on opensource project [RocketMQ](https://rocketmq.apache.org/))

Sub module of [ali-sdk](https://github.com/ali-sdk/ali-sdk).

## Install

```bash
npm install ali-ons --save
```

## Usage

consumer

```js
'use strict';

const httpclient = require('urllib');
const Consumer = require('ali-ons').Consumer;
const consumer = new Consumer({
  httpclient,
  accessKeyId: 'your-accessKeyId',
  accessKeySecret: 'your-AccessKeySecret',
  consumerGroup: 'your-consumer-group',
  // namespace: '', // aliyun namespace support
  // isBroadcast: true,
});

consumer.subscribe(config.topic, '*', async msg => {
  console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`)
  // return Consumer.ACTION_RETRY; // you can return ACTION_RETRY, then this message will be directly retried
});

consumer.on('error', err => console.log(err));
```

producer

```js
'use strict';
const httpclient = require('urllib');
const Producer = require('ali-ons').Producer;
const Message = require('ali-ons').Message;

const producer = new Producer({
  httpclient,
  accessKeyId: 'your-accessKeyId',
  accessKeySecret: 'your-AccessKeySecret',
  producerGroup: 'your-producer-group',
  // namespace: '', // aliyun namespace support
});

(async () => {
  const msg = new Message('your-topic', // topic
    'TagA', // tag
    'Hello ONS !!! ' // body
  );

  // set Message#keys
  msg.keys = ['key1'];

  // delay consume
  // msg.setStartDeliverTime(Date.now() + 5000);

  const sendResult = await producer.send(msg);
  console.log(sendResult);
})().catch(err => console.error(err))
```

## Secure Keys

Please contact to @gxcsoccer to give you accessKey

- [ons secure data](https://sharelock.io/1/UM02CJiYyhXiZDOn1nhX0iqPqMIQtdwI_T5BY3F-tHs.d8-ycA/01veKH9kgAuFuKCqlVPzGsyPWJ8mQLaKPJjjcB9tpdbvi9L6XQ/IgqDvAdVDMzV9lK2gQzyAj7q-CNk8-1tWrLmdqMV0oJ5qgky40/HgpZyKKDfOGAcyqQ20RUdRgCLRWqF8LUUko0uDl_L-ATNOsi5z/W2bsvBc8tAoqSwNR7u2Sqe6XkNmD98s3UQOK-6T8--VwTbHzcG/dwkHwie3EkGB-TbiMnbRh7_5A-DaOTCtALP3xvl4G0XKxuOriC/2yfuPp7WRucTAoqx2STO5Hv3MZEhh3IXf7YiOQ8pWDDqjLuQSY/_irqzYyeseY9m106ksMUq3-yS_qkBRIuoyL-hHk9ZRhGppsdA5/Dw4Pjg.fmNP3aFkLnvPuhlPRwNcng)

## License

[MIT](LICENSE)
