'use strict';

module.exports = {
  write: true,
  prefix: '^',
  devprefix: '^',
  devdep: [
    'autod',
    'egg-bin',
    'contributors',
    'eslint',
    'eslint-config-egg'
  ],
  exclude: [
    'coverage',
  ],
  test: [
    'test'
  ],
};
