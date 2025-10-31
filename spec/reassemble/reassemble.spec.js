/* eslint-disable no-undef */
/* eslint-disable no-await-in-loop */
const chai = require('chai');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
const { sleep } = require('@elastic.io/component-commons-library');
const reassemble = require('../../lib/actions/reassemble');
const { GroupsProcessor } = require('../../lib/utils');
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

  it('should emit immediately if groupDataSize exceeds MAX_LOCAL_STORAGE_SIZE', async () => {
    const smallData = { content: 'a' };
    const msg = {
      body: {
        groupId: 'largeGroup',
        groupSize: 10,
        timersec: 5000,
        messageData: smallData,
      },
    };
    const cfg = { mode: 'groupSize&timeout', emitAsArray: true };
    const context = getContext();

    const realStringify = JSON.stringify;
    const stringifyStub = sinon.stub(JSON, 'stringify');
    stringifyStub.callsFake((val) => {
      if (val === smallData) {
        return 'a'.repeat(1024 * 1024 * 6); // 6MB
      }
      return realStringify(val);
    });

    await reassemble.process.call(context, msg, cfg);

    expect(context.emit.callCount).to.be.equal(1);
    expect(context.emit.getCall(0).lastArg.body.messageData[0]).to.deep.equal(smallData);

    stringifyStub.restore();
  });
});

describe('GroupsProcessor', () => {
  let context;

  beforeEach(() => {
    context = getContext();
  });

  afterEach(() => {
    sinon.restore();
  });

  it('should not throw an error if checkAndEmitGroup is called concurrently', async () => {
    const cfg = { mode: 'groupSize&timeout', emitAsArray: true };
    const processor = new GroupsProcessor(context, cfg);

    const deleteStub = sinon.stub(processor.maester, 'deleteObjectById').resolves();

    const groupId = 'group123';
    const groupUniqueId = `flow123/step123/${groupId}`;
    processor.groupsStorage[groupUniqueId] = {
      messages: [{ msg1: {} }, { msg2: {} }],
      groupSize: 2,
      maesterId: 'maester-123',
      readyAfter: Date.now() + 10000,
    };

    // Simulate concurrent calls
    const promise1 = processor.checkAndEmitGroup(groupUniqueId);
    const promise2 = processor.checkAndEmitGroup(groupUniqueId);

    await Promise.all([promise1, promise2]);

    expect(deleteStub.callCount).to.be.equal(1);
  });
});
