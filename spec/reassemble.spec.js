const chai = require('chai');
const nock = require('nock');
const sinon = require('sinon');
const logger = require('@elastic.io/component-logger')();
const reassemble = require('../lib/actions/reassemble');

process.env.ELASTICIO_OBJECT_STORAGE_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRJZCI6IjU2YzIwN2FkYjkxMjExODFlNjUwYzBlZiIsImNvbnRyYWN0SWQiOiI1YjVlZDFjZjI3MmNmODAwMTFhZTdiNmEiLCJ3b3Jrc3BhY2VJZCI6IjVhNzFiZmM1NjA3ZjFiMDAwNzI5OGEyYSIsImZsb3dJZCI6IioiLCJ1c2VySWQiOiI1YjE2NGRiMzRkNTlhODAwMDdiZDQ3OTMiLCJpYXQiOjE1ODg1ODg3NjZ9.3GlJAwHz__e2Y5tgkzD1t-JyhgXGJOSVFSLUBCqLh5Y';
process.env.ELASTICIO_WORKSPACE_ID = 'test';
process.env.ELASTICIO_FLOW_ID = 'test';
process.env.ELASTICIO_API_URI = 'https://api.hostname';
process.env.ELASTICIO_OBJECT_STORAGE_URI = 'https://ma.estr';
process.env.ELASTICIO_STEP_ID = 'step_id';

const { expect } = chai;
chai.use(require('chai-as-promised'));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Split on JSONata ', () => {
  let self;

  beforeEach(() => {
    self = {
      emit: sinon.spy(),
      logger,
    };
  });

  after(() => {
    nock.restore();
    nock.cleanAll();
    nock.activate();
  });

  it('Base Case: Group Size is 1', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        messageId: 'msg123',
        groupSize: 1,
      },
    };

    const getMessageGroups = nock('https://ma.estr').get('/objects?query[externalid]=group123').reply(200, []);
    const postMessageGroup = nock('https://ma.estr')
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'group123')
      .reply(200, { objectId: 'group123' });
    const getMessageGroup = nock('https://ma.estr')
      .get('/objects/group123')
      .reply(200, { messages: [], messageIdsSeen: {} });
    const putMessageGroup = nock('https://ma.estr').put('/objects/group123').reply(200, {});
    const getMessageGroup1 = nock('https://ma.estr')
      .get('/objects/group123')
      .reply(200, { messages: [{ msg123: undefined }], messageIdsSeen: { msg123: 'msg123' } });
    const putMessageGroup1 = nock('https://ma.estr').put('/objects/group123').reply(200, {});
    const deleteMessageGroup = nock('https://ma.estr').delete('/objects/group123').reply(200, {});

    await reassemble.process.call(self, msg, { mode: 'groupSize' });
    // eslint-disable-next-line no-unused-expressions
    expect(self.emit.calledOnce).to.be.true;
    expect(self.emit.lastCall.args[1].body).to.deep.equal({
      groupSize: 1,
      groupId: 'group123',
      messageData: {
        msg123: undefined,
        undefined,
      },
    });

    expect(getMessageGroups.isDone()).to.equal(true);
    expect(postMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup.isDone()).to.equal(true);
    expect(putMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup1.isDone()).to.equal(true);
    expect(putMessageGroup1.isDone()).to.equal(true);
    expect(deleteMessageGroup.isDone()).to.equal(true);
  });

  it('Base Case: Group Size is 0', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        messageId: 'msg123',
        groupSize: 0,
      },
    };

    await expect(reassemble.process.call(self, msg, { mode: 'groupSize' })).to.eventually.be.rejectedWith('Size must be a positive integer.');
  });

  it('Interleaved Case with duplicate deliveries', async () => {
    const msgBodies = [
      {
        groupId: '1', groupSize: 3, messageId: '1', messageData: '1-1',
      },
      {
        groupId: '2', groupSize: 2, messageId: '1', messageData: '2-1',
      },
      {
        groupId: '2', groupSize: 2, messageId: '1', messageData: '2-1',
      },
      {
        groupId: '1', groupSize: 3, messageId: '3', messageData: '1-3',
      },
      {
        groupId: '2', groupSize: 2, messageId: '2', messageData: '2-2',
      },
      {
        groupId: '1', groupSize: 3, messageId: '2', messageData: '1-2',
      },
    ];

    // eslint-disable-next-line no-plusplus
    for (let i = 0; i < msgBodies.length; i++) {
      nock('https://ma.estr').get('/objects?query[externalid]=1').reply(200, []);
      nock('https://ma.estr')
        .post('/objects', { messages: [], messageIdsSeen: {} })
        .matchHeader('x-query-externalid', '1')
        .reply(200, { objectId: '1' });
      nock('https://ma.estr')
        .get('/objects/1')
        .reply(200, { messages: [], messageIdsSeen: {} });
      nock('https://ma.estr').put('/objects/1').reply(200, {});
      nock('https://ma.estr')
        .get('/objects/1')
        .reply(200, {
          messages: [{ 1: '1-1' }, { 2: '1-2' }, { 3: '1-3' }],
          messageIdsSeen: { 1: '1', 2: '2', 3: '3' },
        });
      nock('https://ma.estr').put('/objects/1').reply(200, {});
      nock('https://ma.estr').delete('/objects/1').reply(200, {});

      nock('https://ma.estr').get('/objects?query[externalid]=2').reply(200, []);
      nock('https://ma.estr')
        .post('/objects', { messages: [], messageIdsSeen: {} })
        .matchHeader('x-query-externalid', '2')
        .reply(200, { objectId: '2' });
      nock('https://ma.estr')
        .get('/objects/2')
        .reply(200, { messages: [], messageIdsSeen: {} });
      nock('https://ma.estr').put('/objects/2').reply(200, {});
      nock('https://ma.estr')
        .get('/objects/2')
        .reply(200, {
          messages: [{ 1: '2-1' }, { 2: '2-2' }],
          messageIdsSeen: { 1: '1', 2: '2' },
        });
      nock('https://ma.estr').put('/objects/2').reply(200, {});
      nock('https://ma.estr').delete('/objects/2').reply(200, {});

      // eslint-disable-next-line no-await-in-loop
      await reassemble.process.call(self, { body: msgBodies[i] }, { mode: 'groupSize' });
      // eslint-disable-next-line default-case
      switch (i) {
        case i <= 3:
          expect(self.emit.callCount).to.be.equal(0);
          break;
        case 4:
          expect(self.emit.callCount).to.be.equal(5);
          expect(self.emit.lastCall.args[1].body).to.deep.equal({
            groupSize: 2,
            groupId: '2',
            messageData: {
              // 1: '2-1',
              2: '2-2',
              undefined,
            },
          });
          break;
        case 5:
          expect(self.emit.callCount).to.be.equal(6);
          expect(self.emit.lastCall.args[1].body).to.deep.equal({
            groupSize: 3,
            groupId: '1',
            messageData: {
              // 1: '1-1',
              2: '1-2',
              // 3: '1-3',
              undefined,
            },
          });
          break;
      }
    }
  });

  it('Base Case: Group Size is 1, messageData is provided', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        messageId: 'msg123',
        groupSize: 1,
        messageData: {
          id: 1,
        },
      },
    };

    const getMessageGroups = nock('https://ma.estr').get('/objects?query[externalid]=group123').reply(200, []);
    const postMessageGroup = nock('https://ma.estr')
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'group123')
      .reply(200, { objectId: 'group123' });
    const getMessageGroup = nock('https://ma.estr')
      .get('/objects/group123')
      .reply(200, { messages: [], messageIdsSeen: {} });
    const putMessageGroup = nock('https://ma.estr').put('/objects/group123').reply(200, {});
    const getMessageGroup1 = nock('https://ma.estr')
      .get('/objects/group123')
      .reply(200, { messages: [{ msg123: undefined }], messageIdsSeen: { msg123: 'msg123' } });
    const putMessageGroup1 = nock('https://ma.estr').put('/objects/group123').reply(200, {});
    const deleteMessageGroup = nock('https://ma.estr').delete('/objects/group123').reply(200, {});

    await reassemble.process.call(self, msg, { mode: 'groupSize' });
    // eslint-disable-next-line no-unused-expressions
    expect(self.emit.calledOnce).to.be.true;
    expect(self.emit.lastCall.args[1].body).to.deep.equal({
      groupSize: 1,
      groupId: 'group123',
      messageData: {
        msg123: {
          id: 1,
        },
        undefined,
      },
    });

    expect(getMessageGroups.isDone()).to.equal(true);
    expect(postMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup.isDone()).to.equal(true);
    expect(putMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup1.isDone()).to.equal(true);
    expect(putMessageGroup1.isDone()).to.equal(true);
    expect(deleteMessageGroup.isDone()).to.equal(true);
  });

  it('Base Case: No group Group Size, emit after 1000 miliseconds if no incoming message', async () => {
    const msg = {
      body: {
        groupId: 'group123',
        messageId: 'msg123',
        groupSize: undefined,
        timersec: 1000,
      },
    };

    const getMessageGroups = nock('https://ma.estr').get('/objects?query[externalid]=group123').reply(200, []);
    const postMessageGroup = nock('https://ma.estr')
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'group123')
      .reply(200, { objectId: 'group123' });
    const getMessageGroup = nock('https://ma.estr')
      .get('/objects/group123')
      .reply(200, { messages: [], messageIdsSeen: {} });
    const putMessageGroup = nock('https://ma.estr').put('/objects/group123').reply(200, {});
    const getMessageGroup1 = nock('https://ma.estr')
      .get('/objects/group123')
      .reply(200, { messages: [{ msg123: undefined }], messageIdsSeen: { msg123: 'msg123' } });

    const putMessageGroup1 = nock('https://ma.estr').put('/objects/group123').reply(200, {});
    const getMessageGroup2 = nock('https://ma.estr')
      .get('/objects/group123')
      .reply(200, { messages: [{ groupId: 'group123' }], messageIdsSeen: { msg123: 'msg123' } });
    const deleteMessageGroup = nock('https://ma.estr').delete('/objects/group123').reply(200, {});

    await reassemble.process.call(self, msg, { mode: 'timeout' });

    // timersec + 0,5 second
    await sleep(1500);

    // expect(self.emit.calledOnce).to.be.true;
    expect(self.emit.lastCall.args[1].body).to.deep.equal({
      groupSize: 1,
      groupId: 'group123',
      messageData: {
        undefined,
      },
    });

    expect(getMessageGroups.isDone()).to.equal(true);
    expect(postMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup.isDone()).to.equal(true);
    expect(putMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup1.isDone()).to.equal(true);
    expect(putMessageGroup1.isDone()).to.equal(true);
    expect(getMessageGroup2.isDone()).to.equal(true);
    expect(deleteMessageGroup.isDone()).to.equal(true);
  });
});
