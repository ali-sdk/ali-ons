'use strict';

module.exports = {
  info() {},
  warn(...args) {
    console.warn(...args);
  },
  error(...args) {
    console.error(...args);
  },
  debug() {},
};
