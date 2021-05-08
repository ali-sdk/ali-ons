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
  before(() => {
    client = new RemotingClient(Object.assign({ httpclient }, config));
    return client.ready();
  });

  afterEach(mm.restore);

  after(() => client.close());

  it('should create & ready ok', async () => {
    const client = new RemotingClient(Object.assign({ httpclient }, config));
    await client.ready();
    assert(client._namesrvAddrList.length > 0);
    await client.close();
  });

  it('should invoke ok', async () => {
    await client.ready();
    const res = await client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    assert(res);
    assert(res.code === ResponseCode.TOPIC_NOT_EXIST);
  });

  it('should invoke for namesrv with retry', async () => {
    let retryCount = 0;
    mm(client, '_namesrvAddrList', [ 0, 0 ]);
    mm(client, 'invoke', async () => {
      retryCount++;
      mm.restore();
      throw new Error('invoke fail');
    });
    const res = await client.invokeForNameSrvAtLeastOnce(new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    assert(retryCount === 1);
    assert(res);
    assert(res.code === ResponseCode.TOPIC_NOT_EXIST);
  });

  it('should invoke oneway ok', async () => {
    await client.invokeOneway(null, new RemotingCommand({
      code: RequestCode.GET_KV_CONFIG_BY_VALUE,
      customHeader: {
        namespace: 'PROJECT_CONFIG',
        key: '127.0.0.1',
      },
    }));
  });

  it('should close ok', async () => {
    const client = new RemotingClient(Object.assign({ httpclient }, config));
    await client.ready();
    const res = await client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);

    assert(res);
    assert(res.code === ResponseCode.TOPIC_NOT_EXIST);

    await client.close();
    await client.close();
  });

  it('should invoke ok after close', async () => {
    const client = new RemotingClient(Object.assign({ httpclient }, config));
    await client.ready();
    const res = await client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    assert(res);
    assert(res.code === ResponseCode.TOPIC_NOT_EXIST);

    await client.close();

    await client.invokeOneway(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }));
    await client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
  });

  it('should call multi times ok', async () => {
    await client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    await client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    await client.invoke(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }), 5000);
    await client.invokeOneway(null, new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: 'NOT_EXISTS',
      },
    }));
    await client.invokeOneway(null, new RemotingCommand({
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

  it('should support unitName', async () => {
    mm(httpclient, 'request', async url => {
      assert(url.includes('-xxx?nofix=1'));
      return {
        status: 200,
        data: '127.0.0.1:9876;127.0.0.2:9876',
      };
    });
    const client = new RemotingClient(Object.assign({ httpclient, unitName: 'xxx' }, config));
    await client.ready();

    assert(client._namesrvAddrList, [
      '127.0.0.1:9876',
      '127.0.0.2:9876',
    ]);
  });
});
