// eslint-disable-next-line
const { messages } = require('elasticio-node');
const ObjectStorageWrapperExtended = require('./utils-wrapper/ObjectStorageWrapperExtended');

let timeHandle;
var groupList = [];
var groupElements = [{groupSize: undefined, groupId: undefined, messageData:undefined}];

async function timer(this_) {
  for(var i = 1; i < groupElements.length; i++){
    await this_.emit('data', messages.newMessageWithBody( groupElements[i] ))  
  }
}

async function processAction(msg,cfg) {
  const mode = cfg.mode
  const storage = new ObjectStorageWrapperExtended(this);
  const {
    groupSize,
    groupId,
    messageId,
    messageData,
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
  if(mode == 'groupSize'){
    if (groupSize <= 0) {
      throw new Error('Size must be a positive integer.');
    }
  };
  if(mode == 'timeout'){
    if (msg.body.timersec <= 0) {
      throw new Error('Delay timer must be a positive integer.');
    }
  };
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

  const parsedMessageGroup = await storage.lookupParsedObjectById(messageGroupId);
  const filteredMessages = parsedMessageGroup.messages
    .filter((message) => message.messageId !== messageId);
  filteredMessages.push(object);
  parsedMessageGroup.messages = filteredMessages;
  await storage.updateObject(messageGroupId, parsedMessageGroup);
  const messagesNumberSeen = Object.keys(parsedMessageGroup.messageIdsSeen).length;

  this.logger.info(
    `Saw message ${messageId} of group ${groupId}. 
    Currently the group has ${messagesNumberSeen} of ${messageGroupSize} message(s).`,
  );

  // when group sized is defined || when both group size and delay timer are defined
  if(mode == 'groupSize'){
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
  if(mode == 'timeout'){

    // delay timer
    var delay = msg.body.timersec
    if(delay >= 40000){delay == 40000}
    clearTimeout(timeHandle);
    timeHandle = setTimeout(timer, delay, this); 
    
    if(groupList.includes(groupId)){
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      var currentGroup = groupElements.filter(function(x) { return x.groupId == groupId; }); // update incoming messageData in current group
      currentGroup[0].groupSize = messagesNumberSeen
      currentGroup[0].messageData = incomingData
      groupElements = groupElements.filter(function(x) { return x.groupId != groupId; });
      groupElements.push(currentGroup[0]);
    }
    else{
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      groupList.push(groupId)
      groupElements.push({groupSize: messagesNumberSeen, groupId: groupId, messageData:incomingData})
    }
  }

  // When both groupSize and delay timer option is selected 
  if(mode == 'groupSize&timeout'){ 

    var delay = msg.body.timersec
    if(delay >= 40000){delay == 40000}
    clearTimeout(timeHandle);
    timeHandle = setTimeout(timer, delay, this); 

    if(groupList.includes(groupId)){
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      var currentGroup = groupElements.filter(function(x) { return x.groupId == groupId; }); // update incoming messageData in current group
      currentGroup[0].groupSize = messagesNumberSeen
      currentGroup[0].messageData = incomingData
      groupElements = groupElements.filter(function(x) { return x.groupId != groupId; });
      groupElements.push(currentGroup[0]);
    }
    else{
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      groupList.push(groupId)
      groupElements.push({groupSize: messagesNumberSeen, groupId: groupId, messageData:incomingData})
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
      groupList = groupList.filter(def => def != groupId);
      groupElements = groupElements.filter(def => def.groupId != groupId);
    }
  }
}

//----------------------------------------------------------------------------------------------------------------------------
//Dynamic drop-down logic starts hier

async function dynList(cfg) {
  let list = {
      "groupSize": "Use Group Size",
      "timeout": "Use Timeout",
      "groupSize&timeout": "Use Group Size and Timeout"
  };
  return list;
}

async function getMetaModel(cfg) {
  let meta = '';
  if(cfg.mode === "groupSize"){
    meta = {
      "in": {
        "type": "object",
        "required": true,
        "properties": {
          "groupId": {
            "type": "string",
            "required": true,
            "title": "Unique ID to describe the group",
            "order": 5
          },
          "messageId": {
            "type": "string",
            "required": true,
            "title": "Unique ID to describe this message",
            "order": 4
          },
          "groupSize": {
            "type": "number",
            "required": true,
            "title": "Number of messages produced by splitter",
            "order": 3
          },
          "messageData": {
            "title": "Message Data",
            "required": false,
            "type": "object",
            "properties": {},
            "order": 2
          }
        }
      },
      "out": {
        "type": "object"
      }
    };
  }
  else if(cfg.mode === "timeout"){
    meta = {
      "in": {
        "type": "object",
        "required": true,
        "properties": {
          "groupId": {
            "type": "string",
            "required": true,
            "title": "Unique ID to describe the group",
            "order": 5
          },
          "messageId": {
            "type": "string",
            "required": true,
            "title": "Unique ID to describe this message",
            "order": 4
          },
          "timersec": {
            "type": "number",
            "required":  true,
            "help":{
              "description": "Time the process waits when no incoming messages before emiting(Default 20000 miliseconds)"
            },
            "title": "Delay timer(in ms)",
            "order": 3
          },
          "messageData": {
            "title": "Message Data",
            "required": false,
            "type": "object",
            "properties": {},
            "order": 2
          }
        }
      },
      "out": {
        "type": "object"
      }
    };
  }
  else if(cfg.mode === "groupSize&timeout"){
    meta = {
      "in": {
        "type": "object",
        "required": true,
        "properties": {
          "groupId": {
            "type": "string",
            "required": true,
            "title": "Unique ID to describe the group",
            "order": 5
          },
          "messageId": {
            "type": "string",
            "required": true,
            "title": "Unique ID to describe this message",
            "order": 4
          },
          "groupSize": {
            "type": "number",
            "required": false,
            "title": "Number of messages produced by splitter",
            "order": 3
          },
          "timersec": {
            "type": "number",
            "required": false,
            "help":{
              "description": "Time the process waits when no incoming messages before emiting(Default 20000 miliseconds)"
            },
            "title": "Delay timer(in ms)",
            "order": 2
          },
          "messageData": {
            "title": "Message Data",
            "required": false,
            "type": "object",
            "properties": {},
            "order": 1
          }
        }
      },
      "out": {
        "type": "object"
      }
    };
  }
  return meta;
}
module.exports = {
  process: processAction,
  getMetaModel: getMetaModel,
  dynList: dynList
}
