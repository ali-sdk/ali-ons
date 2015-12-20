'use strict';

const fs = require('fs');
const path = require('path');
const fixtures = path.join(__dirname, 'fixtures');

exports.bytes = name => {
  return fs.readFileSync(path.join(fixtures, name));
};

exports.write = (name, buf) => {
  fs.writeFileSync(path.join(fixtures, name), buf);
};
