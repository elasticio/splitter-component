const arrayOfObjects = require('./splitArrayOfObjects.json');
const splitArrayOfArrays = require('./splitArrayOfArrays.json');
const arrayOfArraysOfObjects = require('./arrayOfArraysOfObjects.json');
const arrayOfArraysOfPrimitives = require('./arrayOfArraysOfPrimitives.json');
const rootArray = require('./rootArray.json');
const nothingToSplit = require('./nothingToSplit.json');
const noSuchPropertySplitting = require('./noSuchPropertySplitting.json');
const arrayOfPrimitives = require('./arrayOfPrimitives.json');
const primitiveValue = require('./primitiveValue.json');
const objectValue = require('./objectValue.json');

module.exports = {
  'should split array of objects users': arrayOfObjects,
  'should split array of arrays users.friends': splitArrayOfArrays,
  'should split array of arrays of objects users.friends': arrayOfArraysOfObjects,
  'should split array of arrays of primitives userNames': arrayOfArraysOfPrimitives,
  'should split root array': rootArray,
  'should emit only "end" with empty array userNames': nothingToSplit,
  'should emit error no such property users.emails': noSuchPropertySplitting,
  'should emit error for array of primitives userNames': arrayOfPrimitives,
  'should emit error when users is primitive': primitiveValue,
  'should emit object when users is object': objectValue,
};
