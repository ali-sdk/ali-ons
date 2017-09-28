'use strict';

const mm = require('mm');
const assert = require('assert');
const httpclient = require('urllib');
const config = require('../example/config');
const RemotingClient = require('../lib/remoting_client');
const RequestCode = require('../lib/protocol/request_code');
const ResponseCode = require('../lib/protocol/response_code');
const RemotingCommand = require('../lib/protocol/command/remoting_command');

describe('test/remoting_client.test.js', function() {
  let client;
  before(function* () {
    client = new RemotingClient(Object.assign({ httpclient }, config));
    yield client.ready();
  });

  afterEach(mm.restore);

  after(function* () {
    yield client.close();
  });

  it('should create & ready ok', function* () {
    const client = new RemotingClient(Object.assign({ httpclient }, config));
    yield client.ready();
    assert(client._namesrvAddrList.length > 0);
    yield client.close();
  });

  it('should invoke ok', function* () {
    yield client.ready();
    const res = yield client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    assert(res);
    assert(res.code === ResponseCode.TOPIC_NOT_EXIST);
  });

  it('should invoke oneway ok', function* () {
    yield client.invokeOneway(null, new RemotingCommand({
      code: RequestCode.GET_KV_CONFIG_BY_VALUE,
      customHeader: {
        namespace: 'PROJECT_CONFIG',
        key: '127.0.0.1',
      },
    }));
  });

  it('should close ok', function* () {
    const client = new RemotingClient(Object.assign({ httpclient }, config));
    yield client.ready();
    const res = yield client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);

    assert(res);
    assert(res.code === ResponseCode.TOPIC_NOT_EXIST);

    yield client.close();
    yield client.close();
  });

  it('should invoke ok after close', function* () {
    const client = new RemotingClient(Object.assign({ httpclient }, config));
    yield client.ready();
    const res = yield client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    assert(res);
    assert(res.code === ResponseCode.TOPIC_NOT_EXIST);

    yield client.close();

    yield client.invokeOneway(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }));
    yield client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
  });

  it('should call multi times ok', function* () {
    yield client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    yield client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    yield client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    yield client.invokeOneway(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }));
    yield client.invokeOneway(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }));
  });

  // it('should not create two channel with same address', function() {
  //   const address = client._namesrvAddrList[0];
  //   assert(client.getAndCreateChannel(address).clientId === client.getAndCreateChannel(address).clientId);
  // });

  it('should create a new channel if old one is closed', function() {
    const address = client._namesrvAddrList[0];
    const channel = client.getAndCreateChannel(address);
    channel.close();
    assert(client.getAndCreateChannel(address).clientId !== channel.clientId);
  });
});
