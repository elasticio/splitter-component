/* eslint-disable import/first */
process.env.ELASTICIO_OBJECT_STORAGE_TOKEN = 'token';
process.env.ELASTICIO_OBJECT_STORAGE_URI = 'https://api.hostname';
const sinon = require('sinon');
const getLogger = require('@elastic.io/component-logger');

exports.getContext = () => ({
  logger: getLogger(),
  emit: sinon.spy(),
});
