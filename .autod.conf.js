'use strict';

module.exports = {
  write: true,
  prefix: '^',
  devprefix: '^',
  devdep: [
    'autod',
    'egg-bin',
    'egg-ci',
    'contributors',
    'eslint',
    'eslint-config-egg'
  ],
  exclude: [
    '_lib',
  ],
  test: [
    'test'
  ],
};
