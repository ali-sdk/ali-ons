'use strict';
/* eslint no-bitwise: 0 */

exports.CompressedFlag = 1 << 0;
exports.MultiTagsFlag = 1 << 1;

/**
 * 7 6 5 4 3 2 1 0
 * SysFlag 事务相关，从左属，2与3
 */
exports.TransactionNotType = 0 << 2;
exports.TransactionPreparedType = 1 << 2;
exports.TransactionCommitType = 2 << 2;
exports.TransactionRollbackType = 3 << 2;

exports.getTransactionValue = function(flag) {
  return flag & exports.TransactionRollbackType;
};

exports.resetTransactionValue = function(flag, type) {
  return flag & ~exports.TransactionRollbackType | type;
};

exports.clearCompressedFlag = function(flag) {
  return flag & ~exports.CompressedFlag;
};
