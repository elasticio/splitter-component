'use strict';
const debug = require('debug')('splitter');
const _ = require('lodash');
const messages = require('elasticio-node').messages;
const MANY = '[*]';

/**
 * Returns count of splits
 * For example if input is {property: "v1[*].v15[*].v2"} returns 2
 * @param splitting is a string representing a splitting expression
 * @returns Number
 */
function getSplittingLevel(splitting) {
    let depth = 0;
    let arrays = splitting.match(/\[\*\]/g);

    if (!arrays) {
        return depth;
    }

    if (arrays.length > depth) {
        depth = arrays.length;
    }
    return depth;
}

/**
 * Checks if variable key representing array
 * For example if input is 'v1[*]' return true
 * @param key
 * @returns Boolean
 */
function isArrayKey(key) {
    return typeof key !== 'number' && key.indexOf(MANY) > 0;
}

/**
 * Convert string to simple key
 * For example if input is 'v1[*]' returns 'v1'
 * @param key
 * @returns String
 */
function toSingleKey(key) {
    return key.substr(0, key.lastIndexOf(MANY));
}

/**
 * Special clone function that do the deep clone of the data
 * NOTE: all functions that were part of the object data will be lost
 *
 * @param a
 * @returns {*}
 */
function clone(a) {
    return JSON.parse(JSON.stringify(a));
}

/**
 * This function will do the multiplication of the values
 * along the given path.
 *
 * @param path an array of values ['commits','files','file']
 * @param root a source object
 * @param index an optional variable used internally for recursion, if absent will be set to 0
 * @returns {*} an array of value cloned (multiplied) along the path so that all arrays along the path are replaced with
 *  simple values
 */
function doSplit(path, root, index) {
    // default index is 0
    index = index || 0;
    // End of recursion?
    if (index === path.length) {
        return [root];
    }

    // Resulting array
    let currentPath = path[index];
    // Getting value on the path
    let array = root[currentPath];
    if (!array) {
        // Can't find next value along the path
        // will just return latest value
        return root;
    }

    if (!Array.isArray(array)) {
        // Apparently next value along the path
        // is not an array, so we just going one
        // step further along the path
        return doSplit(path, root, index + 1);
    }

    let results = [];
    // We have a value and it's an Array
    array.forEach((arrayValue) => {

        let result = arrayValue;

        if (index < path.length) {

            result = doSplit(path, result, index + 1);

        }

        results = results.concat(result);

    });

    return results;
}

/* Returns array which represents a path
 * For example if input is ['v1[*]','v2']
 * with splitting lvl 1 returns ['v1', 'v2']
 * @param path is array
 * @param splittingLevel is a count of splits
 * @returns Array
 */
function cutByMaxSplittingLevel(path, splittingLevel) {
    let counter = 0;
    let result = [];

    path.forEach((node) => {
        if (isArrayKey(node)) {
            counter++;
            if (counter <= splittingLevel) {
                result.push(toSingleKey(node));
            }
        } else {
            result.push(node);
        }
    });

    return result;
}


function splitMessage(message, splitting, splittingLevel) {

    let loopPath = splitting.split('.');
    loopPath = cutByMaxSplittingLevel(loopPath, splittingLevel);


    let results = [];
    let res = doSplit(loopPath, message);

    res.forEach((root) => {
        results = results.concat(doSplit(loopPath, root));
    });

    return results;
}

/**
 * Returns missing property if splitting has it but object doesn't
 * Returns undefined if all properties in splitting are valid
 * @param body is plain object
 * @param splitting is a string representing a splitting expression
 * @param splittingLevel is a number that means count of splits
 * @returns {String|undefined}
 */
function findMissingProperty(body, splitting, splittingLevel) {
    const pathChunks = splitting.split('.');
    const path = cutByMaxSplittingLevel(pathChunks, splittingLevel);

    let result = body;
    let i = 0;

    let property;
    while (i < path.length && result) {
        property = path[i];
        if (_.isArray(result)) {
            result = result[0][property];
        } else if (_.isPlainObject(result)) {
            result = result[property];
        } else {
            break;
        }

        if (result && !_.isArray(result)) {
            debug(`"${property}" property in the splitting expression is not an array. Returning the original object`);
        }

        i++;
    }

    if (!result) {
        return property;
    }
}

/** @this processAction */
function processAction(msg, conf) {
    const splitting = conf.splitter || {};
    let body = msg.body || {};

    debug('Received new message with body: %j', body);
    debug('Config: %j', conf);

    const splittingLevel = getSplittingLevel(splitting);
    const property = findMissingProperty(body, splitting, splittingLevel);

    if (splittingLevel <= 0 || !isArrayKey(splitting)) {
        this.emit('error', new Error(
            `The splitting expression "${splitting}" should contain at least one "[*]"`
        ));
        return;
    }

    if (property) {
        this.emit('error', new Error(
            `The splitting expression "${splitting}": the property "${property}" doesn't exist!`
        ));
        return;
    }

    body = splitMessage(body, splitting, splittingLevel);
    body.forEach((elem) => {
        this.emit('data', messages.newMessageWithBody(elem));
    });
    this.emit('end');
}
exports.process = processAction;
exports._getSplittingLevel = getSplittingLevel;
exports._cutByMaxSplittingValue = cutByMaxSplittingLevel;
exports._findMissingProperty = findMissingProperty;
exports._splitMessage = splitMessage;
