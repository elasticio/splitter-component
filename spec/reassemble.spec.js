const chai = require('chai');
const nock = require('nock');
const sinon = require('sinon');
const logger = require('@elastic.io/component-logger')();
const reassemble = require('../lib/actions/reassemble');

const objectStorageUri = 'https://ma.estr';

process.env.ELASTICIO_OBJECT_STORAGE_TOKEN = 'token';
process.env.ELASTICIO_WORKSPACE_ID = 'test';
process.env.ELASTICIO_FLOW_ID = 'test';
process.env.ELASTICIO_API_URI = 'https://api.hostname';
process.env.ELASTICIO_OBJECT_STORAGE_URI = objectStorageUri;
process.env.ELASTICIO_STEP_ID = 'step_id';

const { expect } = chai;
chai.use(require('chai-as-promised'));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Reassemble unit test', () => {
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

    const getMessageGroups = nock(objectStorageUri).get('/objects?query[externalid]=group123').reply(200, []);
    const postMessageGroup = nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'group123')
      .reply(200, { objectId: 'group123' });
    const getMessageGroup = nock(objectStorageUri)
      .get('/objects/group123')
      .reply(200, { messages: [], messageIdsSeen: {} });
    const getMessageGroup1 = nock(objectStorageUri)
      .get('/objects/group123')
      .reply(200, { messages: [{ msg123: undefined }], messageIdsSeen: { msg123: 'msg123' } });
    const putMessageGroup1 = nock(objectStorageUri).put('/objects/group123').reply(200, {});
    const deleteMessageGroup = nock(objectStorageUri).delete('/objects/group123').reply(200, {});

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

  xit('Interleaved Case with duplicate deliveries', async () => {
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
      nock(objectStorageUri).get('/objects?query[externalid]=1').reply(200, []);
      nock(objectStorageUri)
        .post('/objects', { messages: [], messageIdsSeen: {} })
        .matchHeader('x-query-externalid', '1')
        .reply(200, { objectId: '1' });
      nock(objectStorageUri)
        .get('/objects/1')
        .reply(200, { messages: [], messageIdsSeen: {} });
      nock(objectStorageUri).put('/objects/1').reply(200, {});
      nock(objectStorageUri)
        .get('/objects/1')
        .reply(200, { messages: [{ messageId: '1', groupId: '1', messageData: '1-1' }], messageIdsSeen: { 1: '1' } });
      nock(objectStorageUri).put('/objects/1').reply(200, {});
      nock(objectStorageUri).delete('/objects/1').reply(200, {});

      nock(objectStorageUri).get('/objects?query[externalid]=2').reply(200, []);
      nock(objectStorageUri)
        .post('/objects', { messages: [], messageIdsSeen: {} })
        .matchHeader('x-query-externalid', '2')
        .reply(200, { objectId: '2' });
      nock(objectStorageUri)
        .get('/objects/2')
        .reply(200, { messages: [], messageIdsSeen: {} });
      nock(objectStorageUri).put('/objects/2').reply(200, {});
      nock(objectStorageUri)
        .get('/objects/2')
        .reply(200, { messages: [{ messageId: '1', groupId: '2', messageData: '2-1' }], messageIdsSeen: { 1: '1' } });
      nock(objectStorageUri).put('/objects/2').reply(200, {});
      nock(objectStorageUri).delete('/objects/2').reply(200, {});

      // eslint-disable-next-line no-await-in-loop
      await reassemble.process.call(self, { body: msgBodies[i] }, { mode: 'groupSize' });
      // eslint-disable-next-line default-case
      switch (i) {
        case i <= 3:
          expect(self.emit.callCount).to.be.equal(0);
          break;
        case 4:
          expect(self.emit.callCount).to.be.equal(1);
          expect(self.emit.lastCall.args[1].body).to.deep.equal({
            groupSize: 2,
            groupId: '2',
            messageData: {
              1: '2-1',
              2: '2-2',
            },
          });
          break;
        case 5:
          expect(self.emit.callCount).to.be.equal(2);
          expect(self.emit.lastCall.args[1].body).to.deep.equal({
            groupSize: 3,
            groupId: '1',
            messageData: {
              1: '1-1',
              2: '1-2',
              3: '1-3',
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

    const getMessageGroups = nock(objectStorageUri).get('/objects?query[externalid]=group123').reply(200, []);
    const postMessageGroup = nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'group123')
      .reply(200, { objectId: 'group123' });
    const getMessageGroup = nock(objectStorageUri)
      .get('/objects/group123')
      .reply(200, { messages: [], messageIdsSeen: {} });
    const putMessageGroup = nock(objectStorageUri).put('/objects/group123').reply(200, {});
    const getMessageGroup1 = nock(objectStorageUri)
      .get('/objects/group123')
      .reply(200, { messages: [{ messageId: 'msg123', groupId: 'group123', messageData: { id: 1 } }], messageIdsSeen: { msg123: 'msg123' } });
    const deleteMessageGroup = nock(objectStorageUri).delete('/objects/group123').reply(200, {});

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
      },
    });

    expect(getMessageGroups.isDone()).to.equal(true);
    expect(postMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup.isDone()).to.equal(true);
    expect(putMessageGroup.isDone()).to.equal(true);
    expect(getMessageGroup1.isDone()).to.equal(true);
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

    const getMessageGroups = nock(objectStorageUri).get('/objects?query[externalid]=group123').reply(200, []);
    const postMessageGroup = nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'group123')
      .reply(200, { objectId: 'group123' });
    const getMessageGroup = nock(objectStorageUri)
      .get('/objects/group123')
      .reply(200, { messages: [], messageIdsSeen: {} });
    const putMessageGroup = nock(objectStorageUri).put('/objects/group123').reply(200, {});
    const getMessageGroup1 = nock(objectStorageUri)
      .get('/objects/group123')
      .reply(200, { messages: [{ msg123: undefined }], messageIdsSeen: { msg123: 'msg123' } });

    const getMessageGroup2 = nock(objectStorageUri)
      .get('/objects/group123')
      .reply(200, { messages: [{ groupId: 'group123' }], messageIdsSeen: { msg123: 'msg123' } });
    const deleteMessageGroup = nock(objectStorageUri).delete('/objects/group123').reply(200, {});

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
    expect(getMessageGroup2.isDone()).to.equal(true);
    expect(deleteMessageGroup.isDone()).to.equal(true);
  });

  it('Base Case: Group Size is 2, with different messageId and messageData', async () => {
    const msg = [
      {
        groupId: 'a', groupSize: 2, messageId: '1', messageData: '1-1',
      },
      {
        groupId: 'a', groupSize: 2, messageId: '2', messageData: '1-2',
      },
    ];

    // First Run
    nock(objectStorageUri).get('/objects?query[externalid]=a').reply(200, []);
    nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'a')
      .reply(200, { objectId: 'a' });
    nock(objectStorageUri)
      .get('/objects/a')
      .reply(200, { messages: [], messageIdsSeen: {} });
    nock(objectStorageUri).put('/objects/a').reply(200, {});
    nock(objectStorageUri)
      .get('/objects/a')
      .reply(200, {
        messages: [{
          groupSize: 2, messageId: '1', groupId: 'a', messageData: '1-1',
        }],
        messageIdsSeen: { 1: '1' },
      });
    nock(objectStorageUri).put('/objects/a').reply(200, {});
    nock(objectStorageUri).delete('/objects/a').reply(200, {});

    // Second  Run
    nock(objectStorageUri).get('/objects?query[externalid]=a').reply(200, []);
    nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'a')
      .reply(200, { objectId: 'a' });
    nock(objectStorageUri)
      .get('/objects/a')
      .reply(200, { messages: [], messageIdsSeen: {} });
    nock(objectStorageUri).put('/objects/a').reply(200, {});
    nock(objectStorageUri)
      .get('/objects/a')
      .reply(200, {
        messages: [{
          groupSize: 2, groupId: '1', messageId: '1', messageData: '1-1',
        }, {
          groupSize: 2, groupId: '2', messageId: '2', messageData: '1-2',
        }],
        messageIdsSeen: { 1: '1', 2: '2' },
      });
    nock(objectStorageUri).put('/objects/a').reply(200, {});
    nock(objectStorageUri).delete('/objects/a').reply(200, {});

    for (let i = 0; i < msg.length; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await reassemble.process.call(self, { body: msg[i] }, { mode: 'groupSize' });

      // eslint-disable-next-line default-case
      switch (i) {
        case 0:
          expect(self.emit.callCount).to.be.equal(0);
          break;
        case 1:
          expect(self.emit.callCount).to.be.equal(1);
          expect(self.emit.lastCall.args[1].body).to.deep.equal({
            groupSize: 2,
            groupId: 'a',
            messageData: {
              1: '1-1',
              2: '1-2',
            },
          });
          break;
      }
    }
  });

  it('Base Case: Group Size is 2, with messageId UNDEFINED and messageData defined', async () => {
    const msg = [
      {
        groupId: 'b', groupSize: 2, messageData: '1-1',
      },
      {
        groupId: 'b', groupSize: 2, messageData: '1-2',
      },
    ];

    // First Run
    nock(objectStorageUri).get('/objects?query[externalid]=b').reply(200, []);
    nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'b')
      .reply(200, { objectId: 'b' });
    nock(objectStorageUri)
      .get('/objects/b')
      .reply(200, { messages: [], messageIdsSeen: {} });
    nock(objectStorageUri).put('/objects/b').reply(200, {});
    nock(objectStorageUri)
      .get('/objects/b')
      .reply(200, {
        messages: [{
          groupSize: 2, groupId: 'b', messageData: '1-1',
        }],
        messageIdsSeen: { 1: '1' },
      });
    nock(objectStorageUri).put('/objects/b').reply(200, {});
    nock(objectStorageUri).delete('/objects/b').reply(200, {});

    // Second  Run
    nock(objectStorageUri).get('/objects?query[externalid]=b').reply(200, []);
    nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'b')
      .reply(200, { objectId: 'b' });
    nock(objectStorageUri)
      .get('/objects/b')
      .reply(200, { messages: [], messageIdsSeen: {} });
    nock(objectStorageUri).put('/objects/b').reply(200, {});
    nock(objectStorageUri)
      .get('/objects/b')
      .reply(200, {
        messages: [{
          groupSize: 2, groupId: 'b', messageData: '1-1',
        }, {
          groupSize: 2, groupId: 'b', messageData: '1-2',
        }],
        messageIdsSeen: { 1: '1', 2: '2' },
      });
    nock(objectStorageUri).put('/objects/b').reply(200, {});
    nock(objectStorageUri).delete('/objects/b').reply(200, {});

    for (let i = 0; i < msg.length; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await reassemble.process.call(self, { body: msg[i] }, { mode: 'groupSize' });

      // eslint-disable-next-line default-case
      switch (i) {
        case 0:
          expect(self.emit.callCount).to.be.equal(0);
          break;
        case 1:
          expect(self.emit.callCount).to.be.equal(1);
          // eslint-disable-next-line no-case-declarations
          const results = self.emit.lastCall.args[1].body;
          expect(results).to.deep.equal({
            groupSize: 2,
            groupId: 'b',
            messageData: results.messageData,
          });
          break;
      }
    }
  });

  it('Base Case: Using time delay, with messageId UNDEFINED and messageData defined', async () => {
    const msg = [
      {
        groupId: 'c', timersec: 1000, messageData: '1-1',
      },
      {
        groupId: 'c', timersec: 1000, messageData: '1-2',
      },
    ];

    // First Run
    nock(objectStorageUri).get('/objects?query[externalid]=c').reply(200, []);
    nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'c')
      .reply(200, { objectId: 'c' });
    nock(objectStorageUri)
      .get('/objects/c')
      .reply(200, { messages: [], messageIdsSeen: {} });
    nock(objectStorageUri).put('/objects/c').reply(200, {});
    nock(objectStorageUri)
      .get('/objects/c')
      .reply(200, {
        messages: [{
          groupId: 'c', messageData: '1-1',
        }],
        messageIdsSeen: {},
      });
    nock(objectStorageUri).put('/objects/c').reply(200, {});

    // Second  Run
    nock(objectStorageUri).get('/objects?query[externalid]=c').reply(200, []);
    nock(objectStorageUri)
      .post('/objects', { messages: [], messageIdsSeen: {} })
      .matchHeader('x-query-externalid', 'c')
      .reply(200, { objectId: 'c' });
    nock(objectStorageUri)
      .get('/objects/c')
      .reply(200, { messages: [], messageIdsSeen: {} });
    nock(objectStorageUri).put('/objects/c').reply(200, {});
    nock(objectStorageUri)
      .get('/objects/c')
      .reply(200, {
        messages: [{
          groupId: 'c', messageData: '1-1',
        }, {
          groupId: 'c', messageData: '1-2',
        }],
        messageIdsSeen: {},
      });
    nock(objectStorageUri).put('/objects/c').reply(200, {});
    nock(objectStorageUri)
      .get('/objects/c')
      .reply(200, {
        messages: [{
          groupId: 'c', messageData: '1-1',
        }, {
          groupId: 'c', messageData: '1-2',
        }],
        messageIdsSeen: { 1: '1', 2: '2' },
      });
    nock(objectStorageUri).delete('/objects/c').reply(200, {});

    for (let i = 0; i < msg.length; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await reassemble.process.call(self, { body: msg[i] }, { mode: 'timeout' });

      // eslint-disable-next-line default-case
      switch (i) {
        case 0:
          expect(self.emit.callCount).to.be.equal(0);
          break;
        case 1:
          // eslint-disable-next-line no-await-in-loop
          await sleep(1500);
          expect(self.emit.callCount).to.be.equal(1);
          // eslint-disable-next-line no-case-declarations
          const results = self.emit.lastCall.args[1].body;
          expect(results).to.deep.equal({
            groupId: 'c',
            groupSize: 2,
            messageData: results.messageData,
          });
          break;
      }
    }
  });
});
