'use strict';

const assert = require('assert');
const PullSysFlag = require('../../lib/utils/pull_sys_flag');

describe('test/utils/pull_sys_flag.test.js', () => {

  it('should buildSysFlag ok', () => {
    assert(PullSysFlag.buildSysFlag(true, true, true, true) === 15);
    assert(PullSysFlag.buildSysFlag(false, true, true, true) === 14);
    assert(PullSysFlag.buildSysFlag(false, false, true, true) === 12);
    assert(PullSysFlag.buildSysFlag(false, false, false, true) === 8);
    assert(PullSysFlag.buildSysFlag(false, false, false, false) === 0);

    assert(PullSysFlag.buildSysFlag(false, true, true, true) ===
      PullSysFlag.clearCommitOffsetFlag(PullSysFlag.buildSysFlag(true, true, true, true)));

    assert(PullSysFlag.hasCommitOffsetFlag(PullSysFlag.buildSysFlag(true, true, true, true)));
    assert(!PullSysFlag.hasCommitOffsetFlag(PullSysFlag.buildSysFlag(false, true, true, true)));

    assert(PullSysFlag.hasSuspendFlag(PullSysFlag.buildSysFlag(true, true, true, true)));
    assert(!PullSysFlag.hasSuspendFlag(PullSysFlag.buildSysFlag(false, false, true, true)));

    assert(PullSysFlag.hasSubscriptionFlag(PullSysFlag.buildSysFlag(true, true, true, true)));
    assert(!PullSysFlag.hasSubscriptionFlag(PullSysFlag.buildSysFlag(false, true, false, true)));

    assert(PullSysFlag.hasClassFilterFlag(PullSysFlag.buildSysFlag(true, true, true, true)));
    assert(!PullSysFlag.hasClassFilterFlag(PullSysFlag.buildSysFlag(false, true, true, false)));
  });
});
