// eslint-disable-next-line
const { messages } = require('elasticio-node');
const ObjectStorageWrapperExtended = require('./utils-wrapper/ObjectStorageWrapperExtended');

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

  if (groupSize <= 0) {
    throw new Error('Size must be a positive integer.');
  }
  if (!messageData) {
    incomingData[messageId] = undefined;
  }

  const {
    bucket,
    bucketId,
    bucketSize,
    isCreated,
  } = await storage.createBucketIfNotExists(groupId, groupSize);

  if (isCreated) {
    await storage.createNewObjectInBucket(object, bucketId);
    this.logger.info('New Group created. Added message');
  }
  if (!isCreated) {
    await storage.createNewObjectInBucket(object, bucketId);
    this.logger.info('Existed Group found. Added message');
    this.logger.info(`Previously saved messages: ${Object.keys(bucket.messageIdsSeen).join(', ')}`);
  }

  const parsedBucket = await storage.lookupParsedObjectById(bucketId);
  const filteredMessages = parsedBucket.messages
    .filter((message) => message.messageId !== messageId);
  filteredMessages.push(object);
  parsedBucket.messages = filteredMessages;
  await storage.updateObject(bucketId, parsedBucket);
  const messagesNumberSeen = Object.keys(parsedBucket.messageIdsSeen).length;

  this.logger.info(
    `Saw message ${messageId} of group ${groupId}. 
    Currently the group has ${messagesNumberSeen} of ${bucketSize} message(s).`,
  );

  if (messagesNumberSeen >= bucketSize) {
    parsedBucket.messages.forEach((message) => {
      incomingData[message.messageId] = message.messageData;
    });

    await this.emit('data', messages.newMessageWithBody({
      groupSize,
      groupId,
      messageData: incomingData,
    }));
    await storage.deleteObjectById(bucketId);
    this.logger.info(`Bucket with id ${bucketId} has been deleted`);
  }
}

exports.process = processAction;
