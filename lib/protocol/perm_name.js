'use strict';
/* eslint no-bitwise: 0 */

// var PERM_PRIORITY = 0x1 << 3;
const PERM_READ = 0x1 << 2;
const PERM_WRITE = 0x1 << 1;
const PERM_INHERIT = 0x1 << 0;

exports.PERM_READ = PERM_READ;
exports.PERM_WRITE = PERM_WRITE;

const isReadable = exports.isReadable = function(perm) {
  return (perm & PERM_READ) === PERM_READ;
};

const isWriteable = exports.isWriteable = function(perm) {
  return (perm & PERM_WRITE) === PERM_WRITE;
};

const isInherited = exports.isInherited = function(perm) {
  return (perm & PERM_INHERIT) === PERM_INHERIT;
};

exports.perm2String = function(perm) {
  let str = '';
  str += isReadable(perm) ? 'R' : '-';
  str += isWriteable(perm) ? 'W' : '-';
  str += isInherited(perm) ? 'X' : '-';
  return str;
};
