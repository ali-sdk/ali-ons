'use strict';
/* eslint no-bitwise: 0 */

const FLAG_COMMIT_OFFSET = 1 << 0;
const FLAG_SUSPEND = 1 << 1;
const FLAG_SUBSCRIPTION = 1 << 2;
const FLAG_CLASS_FILTER = 1 << 3;

exports.buildSysFlag = function(commitOffset, suspend, subscription, classFilter) {
  let flag = 0;
  if (commitOffset) {
    flag |= FLAG_COMMIT_OFFSET;
  }

  if (suspend) {
    flag |= FLAG_SUSPEND;
  }

  if (subscription) {
    flag |= FLAG_SUBSCRIPTION;
  }

  if (classFilter) {
    flag |= FLAG_CLASS_FILTER;
  }
  return flag;
};

exports.clearCommitOffsetFlag = function(sysFlag) {
  return sysFlag & ~FLAG_COMMIT_OFFSET;
};

exports.hasCommitOffsetFlag = function(sysFlag) {
  return (sysFlag & FLAG_COMMIT_OFFSET) === FLAG_COMMIT_OFFSET;
};

exports.hasSuspendFlag = function(sysFlag) {
  return (sysFlag & FLAG_SUSPEND) === FLAG_SUSPEND;
};

exports.hasSubscriptionFlag = function(sysFlag) {
  return (sysFlag & FLAG_SUBSCRIPTION) === FLAG_SUBSCRIPTION;
};

exports.hasClassFilterFlag = function(sysFlag) {
  return (sysFlag & FLAG_CLASS_FILTER) === FLAG_CLASS_FILTER;
};
