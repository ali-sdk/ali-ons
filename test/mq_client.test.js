'use strict';

const assert = require('assert');
const httpclient = require('urllib');
const ClientConfig = require('../lib/client_config');
const MQClient = require('../lib/mq_client');

const consumerGroup = 'CID_GXCSOCCER';
const producerGroup = 'PID_GXCSOCCER';

describe('test/mq_client.test.js', () => {
  const config = new ClientConfig(Object.assign({ httpclient }, require('../example/config')));

  let client;
  before(async () => {
    client = MQClient.getAndCreateMQClient(config);
    client.unregisterProducer('CLIENT_INNER_PRODUCER');
    return client.ready();
  });

  after(() => client.close());

  it('should mqclient is singleton', () => {
    assert(MQClient.getAndCreateMQClient(config) === client);
  });

  it('should registerConsumer ok', async () => {
    client.registerConsumer(consumerGroup, {
      subscriptions: new Map(),
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    assert(client._consumerTable.size === 1);
    client.registerConsumer(consumerGroup, {});
    assert(client._consumerTable.size === 1);
    await client.unregisterConsumer(consumerGroup);
    assert(client._consumerTable.size === 0);
  });

  it('should registerProducer ok', async () => {
    client.registerProducer(producerGroup, {
      publishTopicList: new Map(),
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    assert(client._producerTable.size === 1);
    client.registerProducer(producerGroup, {});
    assert(client._producerTable.size === 1);
    await client.unregisterProducer(producerGroup);
    assert(client._producerTable.size === 0);
  });

  it('should not close if there are producer or consumer', async () => {
    client.registerConsumer(consumerGroup, {
      subscriptions: new Map(),
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    await client.close();
    await client.unregisterConsumer(consumerGroup);

    client.registerProducer(producerGroup, {
      publishTopicList: new Map(),
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    await client.close();
    await client.unregisterProducer(producerGroup);
  });

  it('should updateAllTopicRouterInfo ok', async () => {
    const subscriptions = new Map();
    subscriptions.set('TopicTest', {
      topic: 'TopicTest',
      subString: '*',
      classFilterMode: false,
      tagsSet: [],
      codeSet: [],
      subVersion: Date.now(),
    });
    client.registerConsumer(consumerGroup, {
      subscriptions,
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    client.registerConsumer('xxx', null);
    client.registerProducer(producerGroup, {
      publishTopicList: [ 'TopicTest' ],
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    client.registerProducer('xxx', null);

    await client.updateAllTopicRouterInfo();

    await client.unregisterConsumer(consumerGroup);
    await client.unregisterProducer(producerGroup);
    try {
      await client.unregisterConsumer('xxx');
    } catch (err) {
      assert(err.message.includes('resource xxx not created'));
    }
    await client.unregisterProducer('xxx');
  });

  it('should updateTopicRouteInfoFromNameServer ok', async () => {
    client.registerConsumer(config.consumerGroup, {
      subscriptions: new Map(),
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    client.registerProducer(config.producerGroup, {
      publishTopicList: [ 'TopicTest' ],
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    await client.updateTopicRouteInfoFromNameServer('TopicTest');
    let topicRouteData = client._topicRouteTable.get('TopicTest');
    assert(topicRouteData);
    assert(topicRouteData.brokerDatas.length > 0);
    assert(client._brokerAddrTable.size > 0);

    await client.unregisterConsumer(config.consumerGroup);
    await client.unregisterProducer(config.producerGroup);

    await client.updateTopicRouteInfoFromNameServer('TopicTest', true, {
      createTopicKey: 'TopicTest',
      defaultTopicQueueNums: 8,
    });

    topicRouteData = client._topicRouteTable.get('TopicTest');
    assert(topicRouteData);
    assert(topicRouteData.brokerDatas.length > 0);
    assert(client._brokerAddrTable.size > 0);
  });

  it('should topicRouteData2TopicPublishInfo ok', function() {
    let info = client._topicRouteData2TopicPublishInfo('TopicTest', {
      orderTopicConf: 'xxx:8;yyy:8',
    });
    assert(info.orderTopic);
    assert(info.messageQueueList.length === 16);

    info = client._topicRouteData2TopicPublishInfo('TopicTest', {
      brokerDatas: [{
        brokerAddrs: {
          0: '10.10.10.10:1000',
        },
        brokerName: 'xxx',
      }],
      filterServerTable: {},
      queueDatas: [{
        brokerName: 'xxx',
        perm: 6,
        readQueueNums: 8,
        topicSynFlag: 0,
        writeQueueNums: 8,
      }],
    });
    assert(!info.orderTopic);
    assert(info.messageQueueList.length === 8);

    info = client._topicRouteData2TopicPublishInfo('TopicTest', {
      brokerDatas: [{
        brokerAddrs: {
          1: '10.10.10.10:1000',
        },
        brokerName: 'yyy',
      }],
      filterServerTable: {},
      queueDatas: [{
        brokerName: 'yyy',
        perm: 6,
        readQueueNums: 8,
        topicSynFlag: 0,
        writeQueueNums: 8,
      }],
    });
    assert(!info.orderTopic);
    assert(info.messageQueueList.length === 0);
  });

  it('should cleanOfflineBroker ok', function() {
    client._brokerAddrTable.set('fake-broker', {
      0: '127.0.0.1:10911',
    });
    client._cleanOfflineBroker();
    assert(!client._brokerAddrTable.has('fake-broker'));
  });

  it('should sendHeartbeatToAllBroker ok', async () => {
    await client.sendHeartbeatToAllBroker();
    const subscriptions = new Map();
    subscriptions.set(config.consumerGroup, {
      topic: 'TopicTest',
      subString: '*',
      classFilterMode: false,
      tagsSet: [],
      codeSet: [],
      subVersion: Date.now(),
    });
    client.registerConsumer(config.consumerGroup, {
      subscriptions,
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    client.registerProducer(config.producerGroup, {
      publishTopicList: [ 'TopicTest' ],
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    await client.updateTopicRouteInfoFromNameServer('TopicTest');

    const topicRouteData = client._topicRouteTable.get('TopicTest');
    assert(topicRouteData);
    assert(topicRouteData.brokerDatas.length > 0);
    assert(client._brokerAddrTable.size > 0);

    await client.sendHeartbeatToAllBroker();
    await client.unregisterConsumer(config.consumerGroup);
    await client.unregisterProducer(config.producerGroup);
  });

  it('should persistAllConsumerOffset & doRebalance ok', async () => {
    client.registerConsumer(config.consumerGroup, {
      //
      async persistConsumerOffset() { console.log('persistConsumerOffset'); },
      async doRebalance() { console.log('doRebalance'); },
    });
    await client.persistAllConsumerOffset();
    await client.doRebalance();
    // await this.client.notifyConsumerIdsChanged();
    await client.unregisterConsumer(config.consumerGroup);
  });

  // it('should pull message ok', function(done) {
  //   done = pedding(done, 2);
  //   const request = {
  //     consumerGroup: 'please_rename_unique_group_name_1',
  //   };
  //   let i = 0;
  //   this.client.registerConsumer('please_rename_unique_group_name_1', {
  //     pullMessage: function(pullRequest) {
  //       pullRequest.should.eql(request);
  //       if (++i === 2) {
  //         this.client.unregisterConsumer('please_rename_unique_group_name_1');
  //       }
  //       done();
  //     }.bind(this),
  //   });
  //   this.client.executePullRequestImmediately(request);
  //   this.client.executePullRequestLater(request, 100);
  // });
});
