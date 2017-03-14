'use strict';

const MAX_INT_31 = Math.pow(2, 31) - 10;
let opaque = 0;

exports.getNextOpaque = () => {
  if (opaque >= MAX_INT_31) {
    this.resetOpaque();
  }
  return ++opaque;
};

exports.resetOpaque = () => { opaque = 0; };

exports.getCurrentOpaque = () => opaque;
