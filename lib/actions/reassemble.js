const { messages } = require('elasticio-node');

const groupsSeen = {};
const incomingData = {};

async function processAction(msg) {
  const {
    groupSize,
    groupId,
    messageId,
    messageData,
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

  if (messageData) {
    incomingData[messageId] = messageData;
  }

  if (numberSeen >= groupSize) {
    await this.emit('data', messages.newMessageWithBody({
      groupSize,
      groupId,
      messageData: incomingData,
    }));
    delete groupsSeen[groupId];
  }
}

exports.process = processAction;
