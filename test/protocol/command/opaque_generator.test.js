'use strict';

const assert = require('assert');
const OpaqueGenerator = require('../../../lib/protocol/command/opaque_generator');

describe('test/protocol/command/opaque_generator.test.js', function() {

  it('should get current & next opaque', function() {
    OpaqueGenerator.resetOpaque();
    assert(OpaqueGenerator.getNextOpaque() === 1);
    assert(OpaqueGenerator.getCurrentOpaque() === 1);
  });
});
