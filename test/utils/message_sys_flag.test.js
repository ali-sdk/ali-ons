'use strict';

const assert = require('assert');
const MessageSysFlag = require('../../lib/utils/message_sys_flag');

describe('test/utils/message_sys_flag.test.js', () => {

  it('should getTransactionValue ok', () => {
    assert(MessageSysFlag.getTransactionValue(7) === 4);
    assert(MessageSysFlag.getTransactionValue(6) === 4);
    assert(MessageSysFlag.getTransactionValue(15) === 12);
    assert(MessageSysFlag.getTransactionValue(100) === 4);
  });

  it('should resetTransactionValue ok', () => {
    assert(MessageSysFlag.getTransactionValue(MessageSysFlag.resetTransactionValue(7, 0)) === 0);
    assert(MessageSysFlag.getTransactionValue(MessageSysFlag.resetTransactionValue(7, 4)) === 4);
    assert(MessageSysFlag.getTransactionValue(MessageSysFlag.resetTransactionValue(7, 8)) === 8);
    assert(MessageSysFlag.getTransactionValue(MessageSysFlag.resetTransactionValue(7, 12)) === 12);
  });

  it('should clearCompressedFlag ok', () => {
    assert(MessageSysFlag.clearCompressedFlag(7) === 6);
    assert(MessageSysFlag.clearCompressedFlag(6) === 6);
  });
});
