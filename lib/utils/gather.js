'use strict';

const all = require('p-all');

// fns format: [ () => fetch(), () => fetch() ]
module.exports = async function(fns) {
  const concurrency = 5;
  const ret = [];
  await all(fns.map((fn, i) => {
    return () => fn().then(value => {
      ret[i] = { isError: false, value };
    }, error => {
      ret[i] = { isError: true, error };
    });
  }), concurrency);
  return ret;
};
