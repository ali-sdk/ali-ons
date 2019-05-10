'use strict';

const mm = require('mm');
const net = require('net');
const assert = require('assert');
const urllib = require('urllib');
const sleep = require('mz-modules/sleep');
const Channel = require('../lib/channel');
const config = require('../example/config');
const RequestCode = require('../lib/protocol/request_code');
const ResponseCode = require('../lib/protocol/response_code');
const RemotingCommand = require('../lib/protocol/command/remoting_command');
const localIp = require('address').ip();
const onsAddr = 'http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet';

const TOPIC = config.topic;

describe('test/channel.test.js', function() {
  let address;
  before(function(done) {
    urllib.request(onsAddr, function(err, data, result) {
      if (err) {
        return done(err);
      }
      if (result && result.status === 200) {
        address = data.toString().trim().split(';')[0];
        done();
      } else {
        done(new Error('no addresses'));
      }
    });
  });

  afterEach(mm.restore);

  it('should connect ok', () => {
    const channel = new Channel(address);
    return channel.ready();
  });

  it('should close ok', done => {
    const channel = new Channel(address);
    channel.ready(err => {
      if (err) {
        done(err);
        return;
      }
      assert(channel._socket);
      channel.close();
    });
    channel.on('close', () => {
      assert(!channel._socket);
      done();
    });
  });

  it('should invoke ok', async () => {
    const channel = new Channel(address, config);
    await channel.ready();
    const res = await channel.invoke(new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: TOPIC,
      },
    }), 5000);
    assert(res);
    assert(res.body);
    channel.close();
  });

  it('should invoke ok though channel not ready', async () => {
    const channel = new Channel(address);
    const res = await channel.invoke(new RemotingCommand({
      code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
      customHeader: {
        topic: TOPIC,
      },
    }), 5000);
    assert(res);
    assert(res.remark && res.remark.includes('__accessKey is blank'));
    channel.close();
  });

  it('should invoke oneway ok', async () => {
    const channel = new Channel(address);
    await channel.invokeOneway(new RemotingCommand({
      code: RequestCode.GET_KV_CONFIG_BY_VALUE,
      customHeader: {
        namespace: 'PROJECT_CONFIG',
        key: localIp,
      },
    }));
    await channel.ready();
  });

  it('should close if timeout', async () => {
    const server = net.createServer();
    server.listen(9876);
    await sleep(100);

    const channel = new Channel('127.0.0.1:9876');
    await assert.rejects(async () => {
      await channel.invoke(new RemotingCommand({
        code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
        customHeader: {
          topic: TOPIC,
        },
      }), 5000);
    }, {
      name: 'ResponseTimeoutError',
    });
    await sleep(100);

    assert(!channel._socket);

    server.close();
  });

  // it('should emit error if connect failed', function(done) {
  //   done = pedding(done, 2);
  //   const channel = new Channel('100.100.100.100:9876');
  //   channel.once('close', function() {
  //     console.log('channel closed')
  //     done();
  //   });
  //   channel.once('error', function(err) {
  //     console.log('channel error', err);
  //     should.exist(err);
  //     done();
  //   });
  // });

  it('should get error response', async () => {
    const channel = new Channel('100.100.100.100:9876');
    try {
      await channel.invoke(new RemotingCommand({
        code: RequestCode.GET_KV_CONFIG_BY_VALUE,
        customHeader: {
          namespace: 'PROJECT_CONFIG',
          key: localIp,
        },
      }));
    } catch (err) {
      console.log(err);
    }
    channel.close();
  });

  // it.only('should throw error if command is invalid', function*() {
  //   const channel = new Channel(address);
  //   let isError = false;
  //   try {
  //     yield channel.invoke('invalid command', 5000);
  //   } catch (err) {
  //     isError = true;
  //     err.name.should.equal('MQInvalidCommandError');
  //     err.message.should.equal('Invalid command');
  //   }
  //   isError.should.be.ok();
  //   channel.close();
  //   channel.close();
  // });

  it('should clearupInvokes after connection close', async () => {
    const channel = new Channel(address);
    await channel.ready();
    channel.close();
    let isError = false;
    try {
      await channel.invoke(new RemotingCommand({
        code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
        customHeader: {
          topic: 'NOT_EXISTS',
        },
      }), 5000);
    } catch (err) {
      isError = true;
      assert(err.message === `The socket was closed. (address: ${address})`);
    }
    assert(isError);
  });

  // it('should throw error if invoke after close', function(done) {
  //   const channel = new Channel(address);
  //   channel.on('close', function() {
  //     channel.invoke(new RemotingCommand({
  //       code: RequestCode.GET_ROUTEINTO_BY_TOPIC,
  //       customHeader: {
  //         topic: 'NOT_EXISTS'
  //       }
  //     }), 5000, function(err, res) {
  //       should.exist(err);
  //       err.message.should.equal(`The socket is already closed`);
  //       done();
  //     });
  //   });
  //   channel.close();
  // });

  it('should invoke response ok', async () => {
    const channel = new Channel(address);
    await channel.ready();
    await channel.invokeOneway(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 1, ''));
  });

  it('should throw error if invoke timeout', async () => {
    const channel = new Channel(address);
    let isError = false;
    try {
      await channel.invoke(new RemotingCommand({
        code: RequestCode.GET_KV_CONFIG_BY_VALUE,
        customHeader: {
          namespace: 'PROJECT_CONFIG',
          key: localIp,
        },
      }), 1);
    } catch (err) {
      isError = true;
      assert(err.name === 'ResponseTimeoutError');
      assert(err.message === `Server no response in 1ms, address#${address}`);
    }
    channel.close();
    assert(isError);
  });

  // it('should throw error if connection close by remote server', function(done) {
  //   const channel = new Channel(address);
  //   yield channel.ready();
  //   channel.invoke(new RemotingCommand({
  //     code: RequestCode.GET_KV_CONFIG_BY_VALUE,
  //     customHeader: {
  //       namespace: 'PROJECT_CONFIG',
  //       key: localIp,
  //     },
  //   }), 1, function(err, res) {
  //     should.exist(err);
  //     channel.invokes.size.should.equal(0);
  //     err.name.should.equal('MQSocketClosedError');
  //     err.message.should.containEql(`${address} conn closed by remote server`);
  //     done();
  //   });
  //   channel.close();
  // });

});
