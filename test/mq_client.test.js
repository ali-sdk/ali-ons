'use strict';

const assert = require('assert');
const httpclient = require('urllib');
const ClientConfig = require('../lib/client_config');
const MQClient = require('../lib/mq_client');

describe('test/mq_client.test.js', () => {
  const config = new ClientConfig(Object.assign({ httpclient }, require('../example/config')));

  let client;
  before(function* () {
    client = MQClient.getAndCreateMQClient(config);
    yield client.ready();
  });

  after(function* () {
    yield client.close();
  });

  it('should mqclient is singleton', () => {
    assert(MQClient.getAndCreateMQClient(config) === client);
  });

  it('should registerConsumer ok', function* () {
    client.registerConsumer('please_rename_unique_group_name_1', {
      subscriptions: new Map(),
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    assert(client._consumerTable.size === 1);
    client.registerConsumer('please_rename_unique_group_name_1', {});
    assert(client._consumerTable.size === 1);
    yield client.unregisterConsumer('please_rename_unique_group_name_1');
    assert(client._consumerTable.size === 0);
  });

  it('should registerProducer ok', function* () {
    client.registerProducer('please_rename_unique_group_name_1', {
      publishTopicList: new Map(),
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    assert(client._producerTable.size === 1);
    client.registerProducer('please_rename_unique_group_name_1', {});
    assert(client._producerTable.size === 1);
    yield client.unregisterProducer('please_rename_unique_group_name_1');
    assert(client._producerTable.size === 0);
  });

  it('should not close if there are producer or consumer', function* () {
    client.registerConsumer('please_rename_unique_group_name_1', {
      subscriptions: new Map(),
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    yield client.close();
    yield client.unregisterConsumer('please_rename_unique_group_name_1');

    client.registerProducer('please_rename_unique_group_name_1', {
      publishTopicList: new Map(),
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    yield client.close();
    yield client.unregisterProducer('please_rename_unique_group_name_1');
  });

  it('should updateAllTopicRouterInfo ok', function* () {
    const subscriptions = new Map();
    subscriptions.set('please_rename_unique_group_name_1', {
      topic: 'TopicTest',
      subString: '*',
      classFilterMode: false,
      tagsSet: [],
      codeSet: [],
      subVersion: Date.now(),
    });
    client.registerConsumer('please_rename_unique_group_name_1', {
      subscriptions,
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    client.registerConsumer('xxx', null);
    client.registerProducer('please_rename_unique_group_name_1', {
      publishTopicList: [ 'TopicTest' ],
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    client.registerProducer('xxx', null);

    client.updateAllTopicRouterInfo();

    yield client.unregisterConsumer('please_rename_unique_group_name_1');
    yield client.unregisterProducer('please_rename_unique_group_name_1');
    yield client.unregisterConsumer('xxx');
    yield client.unregisterProducer('xxx');
  });

  it('should updateTopicRouteInfoFromNameServer ok', function* () {
    client.registerConsumer('please_rename_unique_group_name_1', {
      subscriptions: new Map(),
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    client.registerProducer('please_rename_unique_group_name_1', {
      publishTopicList: [ 'TopicTest' ],
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    yield client.updateTopicRouteInfoFromNameServer('TopicTest');
    let topicRouteData = client._topicRouteTable.get('TopicTest');
    assert(topicRouteData);
    assert(topicRouteData.brokerDatas.length > 0);
    assert(client._brokerAddrTable.size > 0);

    yield client.unregisterConsumer('please_rename_unique_group_name_1');
    yield client.unregisterProducer('please_rename_unique_group_name_1');

    yield client.updateTopicRouteInfoFromNameServer('TopicTest', true, {
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

  it('should sendHeartbeatToAllBroker ok', function* () {
    yield client.sendHeartbeatToAllBroker();
    const subscriptions = new Map();
    subscriptions.set('please_rename_unique_group_name_1', {
      topic: 'TopicTest',
      subString: '*',
      classFilterMode: false,
      tagsSet: [],
      codeSet: [],
      subVersion: Date.now(),
    });
    client.registerConsumer('please_rename_unique_group_name_1', {
      subscriptions,
      updateTopicSubscribeInfo() {},
      isSubscribeTopicNeedUpdate() {},
      doRebalance() {},
    });
    client.registerProducer('please_rename_unique_group_name_1', {
      publishTopicList: [ 'TopicTest' ],
      updateTopicPublishInfo() {},
      isPublishTopicNeedUpdate() {},
    });
    yield client.updateTopicRouteInfoFromNameServer('TopicTest');

    const topicRouteData = client._topicRouteTable.get('TopicTest');
    assert(topicRouteData);
    assert(topicRouteData.brokerDatas.length > 0);
    assert(client._brokerAddrTable.size > 0);

    yield client.sendHeartbeatToAllBroker();
    yield client.unregisterConsumer('please_rename_unique_group_name_1');
    yield client.unregisterProducer('please_rename_unique_group_name_1');
  });

  it('should persistAllConsumerOffset & doRebalance ok', function* () {
    client.registerConsumer('please_rename_unique_group_name_1', {
      //
      * persistConsumerOffset() { console.log('persistConsumerOffset'); },
      * doRebalance() { console.log('doRebalance'); },
    });
    yield client.persistAllConsumerOffset();
    yield client.doRebalance();
    // yield this.client.notifyConsumerIdsChanged();
    yield client.unregisterConsumer('please_rename_unique_group_name_1');
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
