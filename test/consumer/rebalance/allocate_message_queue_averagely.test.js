'use strict';

const assert = require('assert');
const AllocateMessageQueueAveragely = require('../../../lib/consumer/rebalance/allocate_message_queue_averagely');

describe('test/allocate_message_queue_averagely.test.js', function() {
  const allocator = new AllocateMessageQueueAveragely();
  it('should allocate queue averagely', function() {
    assert(allocator.name === 'AVG');

    let result = allocator.allocate('test', '1', [ 1, 2, 3, 4, 5, 6, 7, 8 ], [ '1', '2' ]);
    assert.deepEqual(result, [ 1, 2, 3, 4 ]);
    result = allocator.allocate('test', '2', [ 1, 2, 3, 4, 5, 6, 7, 8 ], [ '1', '2' ]);
    assert.deepEqual(result, [ 5, 6, 7, 8 ]);
  });

  it('should allocate ok, indivisible', function() {
    let result = allocator.allocate('test', '1', [ 1, 2, 3, 4, 5, 6, 7, 8 ], [ '1', '2', '3' ]);
    assert.deepEqual(result, [ 1, 2, 3 ]);
    result = allocator.allocate('test', '2', [ 1, 2, 3, 4, 5, 6, 7, 8 ], [ '1', '2', '3' ]);
    assert.deepEqual(result, [ 4, 5, 6 ]);
    result = allocator.allocate('test', '3', [ 1, 2, 3, 4, 5, 6, 7, 8 ], [ '1', '2', '3' ]);
    assert.deepEqual(result, [ 7, 8 ]);
  });

  it('should allocate ok, indivisible 2', function() {
    let result = allocator.allocate('test', '1', [ 1, 2, 3, 4, 5 ], [ '1', '2' ]);
    assert.deepEqual(result, [ 1, 2, 3 ]);
    result = allocator.allocate('test', '2', [ 1, 2, 3, 4, 5 ], [ '1', '2' ]);
    assert.deepEqual(result, [ 4, 5 ]);
  });

  it('should allocate ok, indivisible 3', function() {
    let result = allocator.allocate('test', '1', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3' ]);
    assert.deepEqual(result, [ 1, 2 ]);
    result = allocator.allocate('test', '2', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3' ]);
    assert.deepEqual(result, [ 3, 4 ]);
    result = allocator.allocate('test', '3', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3' ]);
    assert.deepEqual(result, [ 5 ]);
  });

  it('should allocate ok, indivisible 4', function() {
    let result = allocator.allocate('test', '1', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4' ]);
    assert.deepEqual(result, [ 1, 2 ]);
    result = allocator.allocate('test', '2', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4' ]);
    assert.deepEqual(result, [ 3 ]);
    result = allocator.allocate('test', '3', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4' ]);
    assert.deepEqual(result, [ 4 ]);
    result = allocator.allocate('test', '4', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4' ]);
    assert.deepEqual(result, [ 5 ]);
  });

  it('should allocate ok, indivisible 5', function() {
    let result = allocator.allocate('test', '1', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5' ]);
    assert.deepEqual(result, [ 1 ]);
    result = allocator.allocate('test', '2', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5' ]);
    assert.deepEqual(result, [ 2 ]);
    result = allocator.allocate('test', '3', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5' ]);
    assert.deepEqual(result, [ 3 ]);
    result = allocator.allocate('test', '4', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5' ]);
    assert.deepEqual(result, [ 4 ]);
    result = allocator.allocate('test', '5', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5' ]);
    assert.deepEqual(result, [ 5 ]);
  });

  it('should allocate ok, indivisible 6', function() {
    let result = allocator.allocate('test', '1', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5', '6' ]);
    assert.deepEqual(result, [ 1 ]);
    result = allocator.allocate('test', '2', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5', '6' ]);
    assert.deepEqual(result, [ 2 ]);
    result = allocator.allocate('test', '3', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5', '6' ]);
    assert.deepEqual(result, [ 3 ]);
    result = allocator.allocate('test', '4', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5', '6' ]);
    assert.deepEqual(result, [ 4 ]);
    result = allocator.allocate('test', '5', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5', '6' ]);
    assert.deepEqual(result, [ 5 ]);
    result = allocator.allocate('test', '6', [ 1, 2, 3, 4, 5 ], [ '1', '2', '3', '4', '5', '6' ]);
    assert.deepEqual(result, []);
  });

  it('should throw error if currentId is null', function() {
    assert.throws(() => {
      allocator.allocate('test', null, [ 1, 2, 3, 4, 5, 6, 7, 8 ], [ '1', '2' ]);
    }, /currentCID is empty/);
  });

  it('should throw error if mqAll is empty', function() {
    assert.throws(() => {
      allocator.allocate('test', '1', [], [ '1', '2' ]);
    }, /mqAll is null or mqAll empty/);
  });

  it('should throw error if cidAll is empty', function() {
    assert.throws(() => {
      allocator.allocate('test', '1', [ 1, 2, 3, 4, 5, 6, 7, 8 ], []);
    }, /cidAll is null or cidAll empty/);
  });

  it('should get empty array if currentId not exists in cidAll', function() {
    const result = allocator.allocate('test', 'not exists', [ 1, 2, 3, 4, 5, 6, 7, 8 ], [ '1', '2' ]);
    assert.deepEqual(result, []);
  });
});
