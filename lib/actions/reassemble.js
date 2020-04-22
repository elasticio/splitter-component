const { messages } = require('elasticio-node');

const groupsSeen = {};

async function processAction(msg) {
  const {
    groupSize,
    groupId,
    messageId,
  } = msg.body;

  if (groupSize <= 0) {
    throw new Error('Size must be a positive integer.');
  }

  if (!groupsSeen[groupId]) {
    groupsSeen[groupId] = {
      groupSize,
      messageIdsSeen: new Set(),
    };
  }

  groupsSeen[groupId].messageIdsSeen.add(messageId);
  const numberSeen = groupsSeen[groupId].messageIdsSeen.size;

  this.logger.info(
    `Saw message ${messageId} of group ${groupId} Currently the group has ${numberSeen} of ${groupSize} message(s).`,
  );

  if (numberSeen >= groupSize) {
    await this.emit('data', messages.newMessageWithBody({
      groupSize,
      groupId,
    }));
    delete groupsSeen[groupId];
  }
}

exports.process = processAction;
