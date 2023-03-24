const chai = require('chai');
const { getContext } = require('./common');
const reassemble = require('../actions/reassemble');

const { expect } = chai;
chai.use(require('chai-as-promised'));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Split on JSONata ', () => {
  let self;
  beforeEach(() => {
    self = getContext();
  });

  it('Base Case: No group Group Size, emit after 1000 miliseconds if no incoming message', async () => {
    const msg = {
      body: {
        groupId: 'group1231',
        messageId: 'msg123',
        groupSize: undefined,
        timersec: 1000,
      },
      passthrough: {
        step_1: {
          headers: {
            reply_to: 'request_reply_key_6411c5631a545c0012b9c55d_7a493b3b-4773-4f1b-80d4-8d81f1cfbcfd',
          },
        },
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
});
