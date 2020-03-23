const _ = require('lodash');
const { JsonataTransform } = require('@elastic.io/component-commons-library');
const { messages } = require('elasticio-node');

/**
 * @this processAction
 */
async function processAction(msg, cfg) {
    const split = JsonataTransform.jsonataTransform(msg, cfg, this);

    if (!Array.isArray(split)) {
        await this.emit('error', new Error('The evaluated expression must be an array.'));
        return;
    }

    if (_.find(split, (elem) => !_.isObject(elem))) {
        await this.emit('error', new Error('Splitting arrays of objects only!'));
        return;
    }

    this.logger.info('Splitting the incoming message into %s messages', split.length);

    for (let i = 0; i < split.length; i += 1) {
        if (split[i]) {
            // eslint-disable-next-line no-await-in-loop
            await this.emit('data', messages.newMessageWithBody(split[i]));
        }
    }
}

exports.process = processAction;
