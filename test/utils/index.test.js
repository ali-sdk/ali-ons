
'use strict';

const assert = require('assert');
const util = require('../../lib/utils/index');

describe('test/utils/index.test.js', () => {
  it('should parseDate ok', () => {
    const d = util.parseDate('2018112000000');
    assert.equal(d.getTime(), 1542643200000);
  });
});
