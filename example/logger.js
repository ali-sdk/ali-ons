'use strict';

module.exports = {
  info() {},
  warn() {},
  error(...args) {
    console.error(...args);
  },
  debug() {},
};
