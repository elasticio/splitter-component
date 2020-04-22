const arrayConstructors = require('./arrayConstructors.json');
const arrayOfArraysOfObjects = require('./arrayOfArraysOfObjects.json');
const arrayOfArraysOfPrimitives = require('./arrayOfArraysOfPrimitives.json');
const arrayOfPrimitives = require('./arrayOfPrimitives.json');
const nothingToSplit = require('./nothingToSplit.json');
const objectConstructors = require('./objectConstructors.json');
const objectValue = require('./objectValue.json');
const primitiveValue = require('./primitiveValue.json');
const rangeQuery = require('./rangeQuery.json');
const rootArray = require('./rootArray.json');
const splitArrayOfArrays = require('./splitArrayOfArrays.json');
const splitArrayOfObjects = require('./splitArrayOfObjects.json');

module.exports = {
  'should split array from an array constructor expression': arrayConstructors,
  'should split array of arrays of objects users.friends': arrayOfArraysOfObjects,
  'should emit error for array of primitives': arrayOfArraysOfPrimitives,
  'should emit error for array of primitives usernames': arrayOfPrimitives,
  'should emit empty array': nothingToSplit,
  'should split array from an object constructor expression': objectConstructors,
  'should emit error for non-array object': objectValue,
  'should emit error for non-array primitive': primitiveValue,
  'should split array from a range query expression': rangeQuery,
  'should split root array': rootArray,
  'should split array of arrays': splitArrayOfArrays,
  'should split array of objects users': splitArrayOfObjects,
};
