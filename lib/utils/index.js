'use strict';

/* eslint no-bitwise:0 */

const zlib = require('zlib');
const is = require('is-type-of');

// 压缩
exports.compress = function(bs) {
  return zlib.deflateSync(bs);
};

// 解压
exports.uncompress = function(bs) {
  return zlib.inflateSync(bs);
};

// 解析日期
exports.parseDate = function(str) {
  return new Date(
    Number(str.slice(0, 4)),
    Number(str.slice(4, 6)) - 1,
    Number(str.slice(6, 8)),
    Number(str.slice(6, 8)),
    Number(str.slice(10, 12)),
    Number(str.slice(12)));
};

// 返回日期时间格式，精度到秒
// 格式如下：20131223051900
exports.timeMillisToHumanString = function(time) {
  const date = is.date(time) ? time : new Date(time);

  return '' +
    date.getFullYear() +
    zeroize(date.getMonth() + 1) +
    zeroize(date.getDate()) +
    zeroize(date.getHours()) +
    zeroize(date.getMinutes()) +
    zeroize(date.getSeconds());
};

// 获取字符串的哈希值
exports.hashCode = function(str) {
  // s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]

  let hash = 0;
  const len = str.length;
  for (let i = 0; i < len; i++) {
    hash = 31 * hash + str.charCodeAt(i) << 0;
  }

  return hash & 4294967295;
};

function zeroize(value, length) {
  if (!length) {
    length = 2;
  }

  value = String(value);
  let zeros = '';
  for (let i = 0; i < length - value.length; i++) {
    zeros += '0';
  }
  return zeros + value;
}
