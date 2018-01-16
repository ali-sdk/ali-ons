ali-ons
=======

[![NPM version][npm-image]][npm-url]
[![build status][travis-image]][travis-url]
[![David deps][david-image]][david-url]
[![node version][node-image]][node-url]

[npm-image]: https://img.shields.io/npm/v/ali-ons.svg?style=flat-square
[npm-url]: https://npmjs.org/package/ali-ons
[travis-image]: https://img.shields.io/travis/ali-sdk/ali-ons.svg?style=flat-square
[travis-url]: https://travis-ci.org/ali-sdk/ali-ons
[david-image]: https://img.shields.io/david/ali-sdk/ali-ons.svg?style=flat-square
[david-url]: https://david-dm.org/ali-sdk/ali-ons
[node-image]: https://img.shields.io/badge/node.js-%3E=_4.2.3-green.svg?style=flat-square
[node-url]: http://nodejs.org/download/

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
  accessKey: 'your-accesskey',
  secretKey: 'your-secretkey',
  consumerGroup: 'your-consumer-group',
  // isBroadcast: true,
});

consumer.subscribe(config.topic, '*', function*(msg) {
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
  accessKey: 'your-accesskey',
  secretKey: 'your-secretkey',
  producerGroup: 'your-producer-group',
});

co(function*() {
  const msg = new Message('your-topic', // topic
    'TagA', // tag
    'Hello ONS !!! ' // body
  );

  // set Message#keys
  msg.keys = ['key1'];

  const sendResult = yield producer.send(msg);
  console.log(sendResult);
}).catch(err => console.error(err))
```

## License

[MIT](LICENSE)
