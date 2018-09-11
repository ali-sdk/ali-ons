'use strict';

const assert = require('assert');
const address = require('address');
const httpclient = require('urllib');
const MQClientAPI = require('../lib/mq_client_api');

describe('test/mq_client_api.test.js', function() {
  let client;
  before(() => {
    client = new MQClientAPI(Object.assign({ httpclient }, require('../example/config')));
    return client.ready();
  });

  after(() => client.close());

  it('should getProjectGroupByIp ok', () => {
    return client.getProjectGroupByIp(address.ip(), 3000);
  });

  it('should getDefaultTopicRouteInfoFromNameServer ok', async () => {
    const res = await client.getDefaultTopicRouteInfoFromNameServer('TEST_TOPIC', 3000);
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
    const res = await client.getTopicRouteInfoFromNameServer('TEST_TOPIC', 3000);
    assert(res);
  });

  it('should getTopicRouteInfoFromNameServer ok if old one is closed', async () => {
    client._namesrvAddrList.push('1.2.3.4');
    client._namesrvAddrList.push('6.7.8.9');
    const res = await client.getTopicRouteInfoFromNameServer('TEST_TOPIC', 3000);
    assert(res);
  });


});
