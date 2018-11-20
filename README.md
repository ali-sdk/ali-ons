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

Aliyun Open Notification Service Client (base on opensource project [RocketMQ](https://github.com/alibaba/RocketMQ/tree/master/rocketmq-client))

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
  // isBroadcast: true,
});

consumer.subscribe(config.topic, '*', async msg => {
  console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`)
});

consumer.on('error', err => console.log(err));
```

producer

```js
'use strict';
const co = require('co');
const httpclient = require('urllib');
const Producer = require('ali-ons').Producer;
const Message = require('ali-ons').Message;

const producer = new Producer({
  httpclient,
  accessKeyId: 'your-accessKeyId',
  accessKeySecret: 'your-AccessKeySecret',
  producerGroup: 'your-producer-group',
});

(async () => {
  const msg = new Message('your-topic', // topic
    'TagA', // tag
    'Hello ONS !!! ' // body
  );

  // set Message#keys
  msg.keys = ['key1'];

  const sendResult = await producer.send(msg);
  console.log(sendResult);
})().catch(err => console.error(err))
```

## Secure Keys

Please contact to @gxcsoccer to give you accessKey

- [ons secure data](https://sharelock.io/1/JcYdigaQDDbJbFiuUAue6LkmT2pDLAdvWcYZE4A-WKw.Tfy1NC/ry_QLizOWLO_B1_l2OnW7_jRoOH8Avm52oHDLkI9Jq_z5P8va5/GVODvZrDgZL1VvAdzyMO7cKULW25vDle_vsXhPSJdQXul-QM4b/tiv0cYLrLpw9FRJYtT_fcSasEcdtt776WqJ_R1ftC9eg7vtsxD/-CPmBShnD5SG_cEVVZSQuv_geF63l_m6rXPbhKBhHJ3mKGF0_2/yAlpQHVdZA6N5iFlvcMI0ogmXNqkqBGl6yE3-cIqSRZqLSDUd4/EPMhwInVHlL4O9BwM5wYDMT17hiYIaQsXvsGCywGEdjEpLKZdV/7ir9t8RBov0q0FgpcuMrJTvMyQ5dyeoDGzyLm5QTjL8Ty7gqa_/.tFnt_NoGsl3YifWa5BhLnA)

## License

[MIT](LICENSE)
