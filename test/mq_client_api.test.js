'use strict';

const assert = require('assert');
const address = require('address');
const httpclient = require('urllib');
const config = require('../example/config');
const MQClientAPI = require('../lib/mq_client_api');

const TOPIC = config.topic;

describe('test/mq_client_api.test.js', function() {
  let client;
  before(() => {
    client = new MQClientAPI(Object.assign({ httpclient }, Object.assign({}, config, {
      nameSrv: [ 'onsaddr.mq-internet-access.mq-internet.aliyuncs.com:80' ],
    })));
    return client.ready();
  });

  after(() => client.close());

  it('should getProjectGroupByIp ok', () => {
    return client.getProjectGroupByIp(address.ip(), 3000);
  });

  it('should getDefaultTopicRouteInfoFromNameServer ok', async () => {
    const res = await client.getDefaultTopicRouteInfoFromNameServer(TOPIC, 3000);
    assert(res);
  });

  it('should getDefaultTopicRouteInfoFromNameServer for exception', async () => {
    let isError = false;
    try {
      await client.getDefaultTopicRouteInfoFromNameServer('NOT_EXIST_TOPIC', 3000);
    } catch (err) {
      isError = true;
      assert(err.name === 'MQClientException');
    }
    assert(isError);
  });

  it('should getTopicRouteInfoFromNameServer ok', async () => {
    const res = await client.getTopicRouteInfoFromNameServer(TOPIC, 3000);
    assert(res);
  });

  it('should getTopicRouteInfoFromNameServer ok if old one is closed', async () => {
    client._namesrvAddrList.unshift('1.2.3.4:80', '2.3.4.5:80');
    client._namesrvAddrList.push('6.7.8.9:80');
    const res = await client.getTopicRouteInfoFromNameServer(TOPIC, 3000);
    assert(res);
  });

  it('should getTopicRouteInfoFromNameServer ok if old one is closed empty list', async () => {
    client._namesrvAddrList = [];
    let isError = false;
    try {
      await client.getTopicRouteInfoFromNameServer(TOPIC, 3000);
    } catch (err) {
      isError = true;
      assert(err.name === 'MQClientException');
    }
    assert(isError);
  });

});
