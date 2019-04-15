'use strict';

const assert = require('assert');
const RequestCode = require('../../../lib/protocol/request_code');
const ResponseCode = require('../../../lib/protocol/response_code');
const RemotingCommand = require('../../../lib/protocol/command/remoting_command');

describe('test/protocol/command/remoting_command.test.js', function() {

  it('should create request command ok', function() {
    const command = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG_BY_VALUE, {
      namespace: 'PROJECT_CONFIG',
      key: '192.168.1.103',
    });
    assert(command.type === 'REQUEST_COMMAND');
    assert(!command.isResponseType);
    assert(!command.isOnewayRPC);
    command.markOnewayRPC();
    assert(command.isOnewayRPC);

    command.makeCustomHeaderToNet();
    assert.deepEqual(command.decodeCommandCustomHeader(), {
      namespace: 'PROJECT_CONFIG',
      key: '192.168.1.103',
    });
  });

  it('should create response command ok', function() {
    const command = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 1);
    assert(command.type === 'RESPONSE_COMMAND');
    assert(command.isResponseType);
    assert(!command.isOnewayRPC);
    assert(command.remark === 'not set any response code');
  });

  it('should encode command ok', function() {
    const request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG_BY_VALUE, {
      namespace: 'PROJECT_CONFIG',
      key: '192.168.1.103',
    });
    request.opaque = 1;
    assert.deepEqual(request.encode(), Buffer.from('00000085000000817b22636f6465223a3231372c226c616e6775616765223a224a415641222c226f7061717565223a312c22666c6167223a302c2276657273696f6e223a3132312c226578744669656c6473223a7b226e616d657370616365223a2250524f4a4543545f434f4e464947222c226b6579223a223139322e3136382e312e313033227d7d', 'hex'));
    const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 1);
    assert.deepEqual(response.encode(), Buffer.from('00000076000000727b22636f6465223a302c226c616e6775616765223a224a415641222c226f7061717565223a312c22666c6167223a312c2276657273696f6e223a3132312c2272656d61726b223a226e6f742073657420616e7920726573706f6e736520636f6465222c226578744669656c6473223a7b7d7d', 'hex'));
  });

  it('should decode command ok', function() {
    const request = RemotingCommand.decode(Buffer.from('00000084000000807b22636f6465223a3231372c226c616e6775616765223a224a415641222c226f7061717565223a312c22666c6167223a302c2276657273696f6e223a37382c226578744669656c6473223a7b226e616d657370616365223a2250524f4a4543545f434f4e464947222c226b6579223a223139322e3136382e312e313033227d7d', 'hex'));
    assert(request.opaque === 1);
    assert(request.code === RequestCode.GET_KV_CONFIG_BY_VALUE);
    assert.deepEqual(request.extFields, {
      namespace: 'PROJECT_CONFIG',
      key: '192.168.1.103',
    });
    const response = RemotingCommand.decode(Buffer.from('00000066000000627b22636f6465223a302c226c616e6775616765223a224a415641222c226f7061717565223a312c22666c6167223a312c2276657273696f6e223a37382c2272656d61726b223a226e6f742073657420616e7920726573706f6e736520636f6465227d', 'hex'));
    assert(response.opaque === 1);
    assert(response.code === ResponseCode.SUCCESS);
    assert(response.remark === 'not set any response code');
  });
});
