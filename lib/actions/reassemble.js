// eslint-disable-next-line
const { messages } = require('elasticio-node');
const { v4: uuidv4 } = require('uuid');
const ObjectStorageWrapperExtended = require('./utils-wrapper/ObjectStorageWrapperExtended');

let timeHandle;
let groupList = [];
let delay;

async function timer(this_) {
  for (let i = 0; i < groupList.length; i += 1) {
    const storage = new ObjectStorageWrapperExtended(this_);
    // eslint-disable-next-line no-await-in-loop
    const results = await storage.lookupObjectById(groupList[i]);
    const incomingData = {};
    results.messages.forEach((message) => {
      incomingData[message.messageId] = message.messageData;
    });

    // eslint-disable-next-line no-await-in-loop
    await this_.emit('data', messages.newMessageWithBody({
      groupSize: Object.keys(results.messageIdsSeen).length,
      groupId: results.messages[0].groupId,
      messageData: incomingData,
    }));
    // eslint-disable-next-line no-await-in-loop
    await storage.deleteObjectById(groupList[i]);
  }
  groupList = [];
}

async function processAction(msg, cfg) {
  const { mode = 'groupSize' } = cfg;
  const storage = new ObjectStorageWrapperExtended(this);
  const {
    groupSize,
    groupId,
    messageId = uuidv4(),
    messageData,
    timersec = 20000,
  } = msg.body;
  const incomingData = {};
  const object = {
    messageId,
    groupId,
    messageData,
  };

  if (!messageData) {
    incomingData[messageId] = undefined;
  }
  if (mode === 'groupSize') {
    if (groupSize <= 0) {
      throw new Error('Size must be a positive integer.');
    }
  }
  if (mode === 'timeout') {
    if (timersec <= 0) {
      throw new Error('Delay timer must be a positive integer.');
    }
  }
  const {
    messageGroup,
    messageGroupId,
    messageGroupSize,
    isCreated,
  } = await storage.createMessageGroupIfNotExists(groupId, groupSize);

  if (isCreated) {
    await storage.createNewObjectInMessageGroup(object, messageGroupId);
    this.logger.info('New Group created. Added message');
  }
  if (!isCreated) {
    await storage.createNewObjectInMessageGroup(object, messageGroupId);
    this.logger.info('Existed Group found. Added message');
    this.logger.info(`Saved messages: ${Object.keys(messageGroup.messageIdsSeen).join(', ')}`);
  }
  const parsedMessageGroup = await storage.lookupObjectById(messageGroupId);
  const filteredMessages = parsedMessageGroup.messages
    .filter((message) => message.messageId !== messageId);
  filteredMessages.push(object);
  parsedMessageGroup.messages = filteredMessages;
  await storage.updateObjectById(messageGroupId, parsedMessageGroup);
  const messagesNumberSeen = Object.keys(parsedMessageGroup.messageIdsSeen).length;

  this.logger.info(
    `Saw message ${messageId} of group ${groupId}. 
    Currently the group has ${messagesNumberSeen} of ${messageGroupSize} message(s).`,
  );
  // when groupSized option is selected
  if (mode === 'groupSize') {
    if (messagesNumberSeen >= messageGroupSize) {
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      await this.emit('data', messages.newMessageWithBody({
        groupSize,
        groupId,
        messageData: incomingData,
      }));
      await storage.deleteObjectById(messageGroupId);
      this.logger.info(`Message group with id ${messageGroupId} has been deleted`);
    }
  }

  // When delay timer option is selected
  if (mode === 'timeout') {
    delay = (timersec >= 20000) ? 20000 : timersec;
    clearTimeout(timeHandle);
    timeHandle = setTimeout(timer, delay, this);
    if (!groupList.includes(messageGroupId)) {
      groupList.push(messageGroupId);
    }
  }

  // When both groupSize and delay timer option is selected
  if (mode === 'groupSize&timeout') {
    delay = (timersec >= 20000) ? 20000 : timersec;
    clearTimeout(timeHandle);
    timeHandle = setTimeout(timer, delay, this);

    if (!groupList.includes(messageGroupId)) {
      groupList.push(messageGroupId);
    }

    if (messagesNumberSeen >= messageGroupSize) {
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });

      await this.emit('data', messages.newMessageWithBody({
        groupSize,
        groupId,
        messageData: incomingData,
      }));
      await storage.deleteObjectById(messageGroupId);
      this.logger.info(`Message group with id ${messageGroupId} has been deleted`);
      groupList = groupList.filter((def) => def !== messageGroupId);
    }
  }
}

//-------------------------------------------------------------------------------------
// Dynamic drop-down logic starts here

async function getMetaModel(cfg) {
  if (cfg.mode === 'groupSize') {
    return ({
      in: {
        type: 'object',
        required: true,
        properties: {
          groupId: {
            type: 'string',
            required: true,
            title: 'Unique ID to describe the group',
            order: 5,
          },
          messageId: {
            type: 'string',
            required: false,
            title: 'Unique ID to describe this message',
            order: 4,
          },
          groupSize: {
            type: 'number',
            required: true,
            title: 'Number of messages expected to be reassembled into the group',
            order: 3,
          },
          messageData: {
            title: 'Message Data',
            required: false,
            type: 'object',
            properties: {},
            order: 2,
          },
        },
      },
      out: {
        type: 'object',
      },
    });
  }
  if (cfg.mode === 'timeout') {
    return ({
      in: {
        type: 'object',
        required: true,
        properties: {
          groupId: {
            type: 'string',
            required: true,
            title: 'Unique ID to describe the group',
            order: 5,
          },
          messageId: {
            type: 'string',
            required: false,
            title: 'Unique ID to describe this message',
            order: 4,
          },
          timersec: {
            type: 'number',
            required: false,
            help: {
              description: 'Time the process waits when no incoming messages before emiting(Default 20000 miliseconds)',
            },
            title: 'Delay timer(in ms)',
            order: 3,
          },
          messageData: {
            title: 'Message Data',
            required: false,
            type: 'object',
            properties: {},
            order: 2,
          },
        },
      },
      out: {
        type: 'object',
      },
    });
  }
  if (cfg.mode === 'groupSize&timeout') {
    return ({
      in: {
        type: 'object',
        required: true,
        properties: {
          groupId: {
            type: 'string',
            required: true,
            title: 'Unique ID to describe the group',
            order: 5,
          },
          messageId: {
            type: 'string',
            required: false,
            title: 'Unique ID to describe this message',
            order: 4,
          },
          groupSize: {
            type: 'number',
            title: 'Number of messages expected to be reassembled into the group',
            order: 3,
          },
          timersec: {
            type: 'number',
            required: false,
            help: {
              description: 'Time the process waits when no incoming messages before emiting(Default 20000 miliseconds)',
            },
            title: 'Delay timer(in ms)',
            order: 2,
          },
          messageData: {
            title: 'Message Data',
            required: false,
            type: 'object',
            properties: {},
            order: 1,
          },
        },
      },
      out: {
        type: 'object',
      },
    });
  }
  return false;
}
module.exports = {
  process: processAction,
  getMetaModel,
};
