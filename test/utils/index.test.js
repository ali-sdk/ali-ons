'use strict';

const assert = require('assert');
const util = require('../../lib/utils/index');

describe('test/utils/index.test.js', () => {
  it('should parseDate ok', () => {
    const d = util.parseDate('2018112000000');
    assert.equal(d.getTime() - d.getTimezoneOffset() * 60 * 1000, 1542672000000);
  });

  it('should getRetryTopic ok', () => {
    const msg = {
      retryTopic: 'xxx',
    };
    util.resetRetryTopic(msg, 'yyy');
    assert.equal(msg.retryTopic, 'xxx');
  });
});
