const { isNumberNaN } = require('@elastic.io/component-commons-library');
const { GroupsProcessor } = require('../utils');

const MAX_DELAY_TIME = 20000;
let groupsProcessor;

const isDebugFlow = process.env.ELASTICIO_FLOW_TYPE === 'debug';

async function processAction(msg, cfg) {
  groupsProcessor ||= new GroupsProcessor(this, cfg);
  groupsProcessor.setContext(this);

  const {
    groupId,
    messageId,
    messageData,
  } = msg.body;

  let {
    groupSize,
    timersec = MAX_DELAY_TIME,
  } = msg.body;

  if (cfg.mode === 'groupSize' && !groupSize) {
    throw new Error('"Number of messages expected to be reassembled into the group" is required for "Group on fixed amount of messages" behavior');
  }
  if (groupSize && (isNumberNaN(groupSize) || groupSize <= 0)) {
    throw new Error('"Number of messages expected to be reassembled into the group" must be a positive integer.');
  }
  if (timersec && (isNumberNaN(timersec) || timersec <= 0)) {
    throw new Error('"Delay timer (in ms)" must be a positive integer.');
  }
  if (groupSize) {
    groupSize = Number(groupSize);
  } else {
    groupSize = Infinity;
  }
  timersec = Number(timersec);
  if (timersec > MAX_DELAY_TIME) timersec = MAX_DELAY_TIME;

  await groupsProcessor.addMessage({
    groupId,
    messageData,
    groupSize,
    timersec,
    messageId,
  });
  if (isDebugFlow) await groupsProcessor.emitAsIs();
}

async function getMetaModel(cfg) {
  const { mode, emitAsArray } = cfg;

  const result = {
    in: {
      type: 'object',
      properties: {
        groupId: {
          type: 'string',
          required: true,
          title: 'Unique ID to describe the group',
        },
        messageId: {
          type: 'string',
          title: 'Unique ID to describe this message',
        },
        messageData: {
          title: 'Message Data',
          type: 'object',
          properties: {},
        },
      },
    },
    out: {
      type: 'object',
      properties: {
        groupSize: { type: 'number' },
        groupId: { type: 'string' },
        messageData: { type: 'object' },
      },
    },
  };

  if (mode === 'groupSize' || mode === 'groupSize&timeout') {
    result.in.properties.groupSize = {
      type: 'number',
      required: cfg.mode === 'groupSize',
      title: 'Number of messages expected to be reassembled into the group',
    };
  }

  if (mode === 'timeout' || mode === 'groupSize&timeout') {
    result.in.properties.timersec = {
      type: 'number',
      help: {
        description: 'Time the process waits when no incoming messages before emitting (Default and maximum 20000 milliseconds)',
      },
      title: 'Delay timer (in ms)',
    };
  }

  if (emitAsArray) {
    result.out.properties.messageData = {
      type: 'array',
      items: {
        type: 'object',
        properties: {},
      },
    };
  }
  return result;
}
module.exports = {
  process: processAction,
  getMetaModel,
};
