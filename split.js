const _ = require('lodash');
const { messages } = require('elasticio-node');

/** @this processAction */
async function processAction(msg, conf) {
  const splitting = conf.splitter || {};
  const body = msg.body || {};

  this.logger.debug('Received new message');

  const split = splitting === '$' ? body : _.get(body, splitting);

  if (!split) {
    await this.emit('error', new Error(`Could not find properties by following path: "${splitting}"!`));
    return;
  }

  if (!_.isObject(split)) {
    await this.emit('error', new Error('Only objects are accepted!'));
    return;
  }

  if (_.isArray(split) && _.find(split, (elem) => !_.isObject(elem))) {
    await this.emit('error', new Error('Splitting arrays of objects only!'));
    return;
  }

  const results = [];

  if (_.isArray(split)) {
    split.forEach((elem) => results.push(elem));
  } else if (_.isObject(split)) {
    this.logger.debug('Splitting is not an array. Returning the original object');
    results.push(split);
  }

  this.logger.info('Splitting the incoming message into %s messages', results.length);
  results.forEach(async (result) => {
    if (result) {
      await this.emit('data', messages.newMessageWithBody(result));
    }
  });
  await this.emit('end');
}

exports.process = processAction;
