'use strict';
const expect = require('chai').expect;
const sinon = require('sinon');
const splitter = require('../split.js');
const data = require('./data');

describe('Splitter ', () => {
    let self;

    beforeEach(() => {
        self = {
            emit: sinon.spy()
        };
    });

    for (const key of Object.keys(data)) {
        it(key, async () => {
            const { message, config, results } = data[key];
            await splitter.process.call(self, message, config);
            for (let i = 0; i < results.length; i++) {
                const args = self.emit.getCall(i).args;

                const expectedEventType = results[i][0];
                const actualEventType = args[0];
                expect(actualEventType).to.equal(expectedEventType);

                const expected = results[i][1];
                if (!expected) {
                    continue;
                }

                let actual = expectedEventType === 'data'
                    ? args[1].body
                    : args[1];

                if (expectedEventType === 'error') {
                    expect(actual).instanceOf(Error);
                    actual = actual.message;
                }

                expect(actual).to.deep.equal(expected);
            }
        });
    }
});
