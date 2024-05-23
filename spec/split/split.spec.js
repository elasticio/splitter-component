const { expect } = require('chai');
const sinon = require('sinon');
const logger = require('@elastic.io/component-logger')();
const splitter = require('../../split');
const data = require('./data');

describe('Splitter ', () => {
  let self;

  beforeEach(() => {
    self = {
      emit: sinon.spy(),
      logger,
    };
  });

  Object.keys(data).forEach((key) => {
    it(key, async () => {
      const { message, config, results } = data[key];
      await splitter.process.call(self, message, config);
      for (let i = 0; i < results.length; i += 1) {
        const { args } = self.emit.getCall(i);

        const expectedEventType = results[i][0];
        const actualEventType = args[0];
        expect(actualEventType).to.equal(expectedEventType);

        const expected = results[i][1];
        if (expected) {
          let actual = expectedEventType === 'data'
            ? args[1].body
            : args[1];

          if (expectedEventType === 'error') {
            expect(actual).instanceOf(Error);
            actual = actual.message;
          }

          expect(actual).to.deep.equal(expected);
        }
      }
    });
  });
});
