'use strict';

const assert = require('assert');
const address = require('address');
const httpclient = require('urllib');
const MQClientAPI = require('../lib/mq_client_api');

describe('test/mq_client_api.test.js', function() {
  let client;
  before(function* () {
    client = new MQClientAPI(Object.assign({ httpclient }, require('../example/config')));
    yield client.ready();
  });

  after(function* () {
    yield client.close();
  });

  it('should getProjectGroupByIp ok', function* () {
    yield client.getProjectGroupByIp(address.ip(), 3000);
  });

  it('should getDefaultTopicRouteInfoFromNameServer ok', function* () {
    const res = yield client.getDefaultTopicRouteInfoFromNameServer('TEST_TOPIC', 3000);
    assert(res);
  });

  it('should getDefaultTopicRouteInfoFromNameServer for exception', function* () {
    let isError = false;
    try {
      yield client.getDefaultTopicRouteInfoFromNameServer('NOT_EXIST_TOPIC', 3000);
    } catch (err) {
      isError = true;
      assert(err.name === 'MQClientException');
    }
    assert(isError);
  });
});
