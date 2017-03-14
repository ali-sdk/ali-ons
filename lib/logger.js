'use strict';

const chalk = require('chalk');
const Logger = require('egg-logger').Logger;
const ConsoleTransport = require('egg-logger').ConsoleTransport;

const duartionRegexp = /([0-9]+ms)/g;
// eslint-disable-next-line no-useless-escape
const categoryRegexp = /(\[[\w\-_.:]+\])/g;
const httpMethodRegexp = /(GET|POST|PUT|PATH|HEAD|DELETE) /g;

const logger = new Logger();
logger.set('console', new ConsoleTransport({
  level: 'DEBUG',
  formatter(meta) {
    let msg = meta.date + ' ' + meta.level + ' ' + meta.pid + ' ' + meta.message;
    if (!chalk.supportsColor) {
      return msg;
    }

    if (meta.level === 'ERROR') {
      return chalk.red(msg);
    } else if (meta.level === 'WARN') {
      return chalk.yellow(msg);
    }

    msg = msg.replace(duartionRegexp, chalk.green('$1'));
    msg = msg.replace(categoryRegexp, chalk.blue('$1'));
    msg = msg.replace(httpMethodRegexp, chalk.cyan('$1 '));
    return msg;
  },
}));

module.exports = logger;
