/* eslint-disable import/no-extraneous-dependencies */
const getLogger = require('@elastic.io/component-logger');
const sinon = require('sinon');
const { existsSync } = require('fs');
const { config } = require('dotenv');

if (existsSync('.env')) {
  config();
  const {
    ELASTICIO_OBJECT_STORAGE_TOKEN, ELASTICIO_OBJECT_STORAGE_URI,
  } = process.env;
  if (!ELASTICIO_OBJECT_STORAGE_TOKEN || !ELASTICIO_OBJECT_STORAGE_URI) {
    throw new Error('Please, provide all environment variables');
  }
} else {
  throw new Error('Please, provide environment variables to .env');
}

// eslint-disable-next-line import/prefer-default-export
const getContext = () => ({
  logger: getLogger(),
  emit: sinon.spy(),
});

module.exports = {
  getContext,
};
