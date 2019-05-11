'use strict';

// Helper
// -----------
function string2bytes(hexString) {
  if (!hexString) {
    return null;
  }
  return new Buffer(hexString, 'hex');
}

function bytes2string(src) {
  if (!src) {
    return null;
  }
  return src.toString('hex').toUpperCase();
}

module.exports = {
  string2bytes,
  bytes2string,
};
