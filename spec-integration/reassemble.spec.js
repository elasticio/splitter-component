const chai = require('chai');
const nock = require('nock');
const { getContext } = require('./common');
const reassemble = require('../lib/actions/reassemble');

const { expect } = chai;
chai.use(require('chai-as-promised'));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Reassemble integration test', () => {
  let self;
  beforeEach(() => {
    self = getContext();
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
  });

  it('Interleaved Case with duplicate deliveries', async () => {
    nock.recorder.rec();
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
              // undefined,
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
  });
});
