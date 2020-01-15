const _ = require('lodash');
const { JsonataTransform } = require('@elastic.io/component-commons-library');
const { messages } = require('elasticio-node');

/**
 * @this processAction
 */
async function processAction(msg, cfg) {
    const split = JsonataTransform.jsonataTransform(msg, cfg);

    if (!Array.isArray(split)) {
        await this.emit('error', new Error('The evaluated expression must be an array.'));
        return;
    }

    if (_.find(split, (elem) => !_.isObject(elem))) {
        await this.emit('error', new Error('Splitting arrays of objects only!'));
        return;
    }

    const results = [];
    split.forEach((elem) => results.push(elem));
    this.logger.info('Splitting the incoming message into %s messages', results.length);
    results.forEach(async (result) => {
        if (result) {
            await this.emit('data', messages.newMessageWithBody(result));
        }
    });
    await this.emit('end');
}

exports.process = processAction;
