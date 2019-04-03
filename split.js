'use strict';
const debug = require('debug')('splitter');
const _ = require('lodash');
const messages = require('elasticio-node').messages;

/** @this processAction */
async function processAction(msg, conf) {
    const splitting = conf.splitter || {};
    let body = msg.body || {};

    debug('Received new message with body: %j', body);
    debug('Config: %j', conf);

    const split = splitting === '$' ? body : _.get(body, splitting);

    if (!split) {
        await this.emit('error', new Error(`Could not find properties by following path: "${splitting}"!`));
        return;
    }

    if (!_.isObject(split)) {
        await this.emit('error', new Error('Only objects are accepted!'));
        return;
    }

    if (_.isArray(split) && _.find(split, elem => !_.isObject(elem))) {
        await this.emit('error', new Error('Splitting arrays of objects only!'));
        return;
    }

    const result = [];

    if (_.isArray(split)) {
        split.forEach(elem => result.push(elem));
    } else if (_.isObject(split)) {
        debug(`"${splitting}" is not an array. Returning the original object`);
        result.push(split);
    }

    debug('%s parts to emit found', result.length);
    for (let value of result) {
        if (value) {
            await this.emit('data', messages.newMessageWithBody(value));
        }
    }
    await this.emit('end');
}

exports.process = processAction;
