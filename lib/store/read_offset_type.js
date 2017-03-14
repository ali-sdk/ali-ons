'use strict';

module.exports = {
  // 只从Memory读取
  READ_FROM_MEMORY: 'READ_FROM_MEMORY',
  // 只从存储层读取（本地或者远端）
  READ_FROM_STORE: 'READ_FROM_STORE',
  // 先从内存读，内存不存在再从存储层读
  MEMORY_FIRST_THEN_STORE: 'MEMORY_FIRST_THEN_STORE',
};
