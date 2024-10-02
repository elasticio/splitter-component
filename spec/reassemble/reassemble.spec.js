/* eslint-disable no-undef */
/* eslint-disable no-await-in-loop */
const chai = require('chai');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
const { sleep } = require('@elastic.io/component-commons-library');
const reassemble = require('../../lib/actions/reassemble');
const { getContext } = require('../common');
const testData = require('./testData.json');

const { expect } = chai;
chai.use(chaiAsPromised);

describe('Reassemble', () => {
  beforeEach(() => {
  });

  afterEach(() => {
    sinon.restore();
  });

  it('mode: groupSize&timeout', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        groupSize: 5,
        timersec: 500,
        messageData: structuredClone(testData),
      },
    };
    const cfg = { mode: 'groupSize&timeout', emitAsArray: true };

    const context = getContext();
    for (let i = 0; i < 11; i += 1) {
      await reassemble.process.call(context, msg, cfg);
    }
    expect(context.emit.callCount).to.be.equal(2);
    expect(context.emit.getCall(0).lastArg.body.messageData.length).to.equal(5);
    expect(context.emit.getCall(0).lastArg.body.messageData[0]).to.deep.equal(testData);
    await sleep(800);
    expect(context.emit.callCount).to.be.equal(3);
    expect(context.emit.getCall(2).lastArg.body.messageData.length).to.equal(1);
  });

  it('mode: groupSize', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        groupSize: 5,
        timersec: 500,
        messageData: structuredClone(testData),
      },
    };
    const cfg = { mode: 'groupSize', emitAsArray: true };

    const context = getContext();
    for (let i = 0; i < 11; i += 1) {
      await reassemble.process.call(context, msg, cfg);
    }
    expect(context.emit.callCount).to.be.equal(2);
    expect(context.emit.getCall(0).lastArg.body.messageData.length).to.equal(5);
    expect(context.emit.getCall(0).lastArg.body.messageData[0]).to.deep.equal(testData);
    await sleep(800);
    expect(context.emit.callCount).to.be.equal(3);
    expect(context.emit.getCall(2).lastArg.body.messageData.length).to.equal(1);
  });

  it('mode: timeout', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        groupSize: 5,
        timersec: 500,
        messageData: structuredClone(testData),
      },
    };
    const cfg = { mode: 'timeout', emitAsArray: true };

    const context = getContext();
    for (let i = 0; i < 3; i += 1) {
      await reassemble.process.call(context, msg, cfg);
    }
    expect(context.emit.callCount).to.be.equal(0);
    await sleep(800);
    expect(context.emit.callCount).to.be.equal(1);
    expect(context.emit.getCall(0).lastArg.body.messageData[0]).to.deep.equal(testData);
  });

  it('emitAsArray: false', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        groupSize: 5,
        timersec: 500,
        messageData: structuredClone(testData),
      },
    };
    const cfg = { mode: 'groupSize&timeout', emitAsArray: true };

    const context = getContext();
    for (let i = 0; i < 5; i += 1) {
      await reassemble.process.call(context, msg, cfg);
    }
    expect(context.emit.callCount).to.be.equal(1);
    const { messageData } = context.emit.getCall(0).lastArg.body;
    expect(Object.values(messageData)[0]).to.deep.equal(testData);
  });
});
