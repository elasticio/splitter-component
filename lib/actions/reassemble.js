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

async function processAction(msg) {
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
  if (groupSize <= 0) {
    throw new Error('Size must be a positive integer.');
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
  if(messageGroupSize !== '' && messageGroupSize !== undefined){
    if(groupList.includes(groupId)){
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      for(var key in groupElements){  
        if(groupElements[key].groupId === groupId){
          groupElements[key].groupSize == messagesNumberSeen
          groupElements[key].messageData == incomingData
        }
      }
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
  // When delay timer is defined and no group size defined 
  if(messageGroupSize === undefined && msg.body.timersec !== undefined){

    // in case no delay was defined  
    var delay = msg.body.timersec
    if(delay >= 40000){delay == 40000}
    clearTimeout(timeHandle);
    timeHandle = setTimeout(timer, delay, this); 
    
    if(groupList.includes(groupId)){
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      for(var key in groupElements){   
        if(groupElements[key].groupId === groupId){
          groupElements[key].groupSize == messagesNumberSeen
          groupElements[key].messageData == incomingData
        }
      }
    }
    else{
      parsedMessageGroup.messages.forEach((message) => {
        incomingData[message.messageId] = message.messageData;
      });
      groupList.push(groupId)
      groupElements.push({groupSize: messagesNumberSeen, groupId: groupId, messageData:incomingData})
    }
  }
  if(messageGroupSize === undefined && msg.body.timersec === undefined){
    throw new Error('Either group size or delay timer need to be defined');
  }
}

exports.process = processAction;
