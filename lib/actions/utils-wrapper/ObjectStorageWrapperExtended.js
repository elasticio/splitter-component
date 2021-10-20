const { ObjectStorageWrapper } = require('@elastic.io/maester-client/dist/ObjectStorageWrapper');

class ObjectStorageWrapperExtended extends ObjectStorageWrapper {
  constructor(context) {
    super(context);
    this.logger = context.logger;
    this.EXTERNAL_ID_QUERY_HEADER_NAME = 'externalid';
    this.TTL_TWO_DAYS = 172800;
  }

  async lookupParsedObjectById(messageGroupId) {
    const messageGroup = await this.lookupObjectById(messageGroupId);
    return JSON.parse(messageGroup);
  }

  async createMessageGroupIfNotExists(externalId, messageGroupSize) {
    this.logger.info('Processing creation of the new message group');
    const messageGroups = await this.lookupObjectsByQueryParameters(
      [{ key: this.EXTERNAL_ID_QUERY_HEADER_NAME, value: externalId }],
    );
    if (messageGroups.length > 1) {
      throw new Error('Several message groups with the same ids can not exist');
    }
    if (!messageGroups.length) {
      this.logger.info('No message groups found');
      const newMessageGroup = {
        messages: [],
        messageIdsSeen: {},
      };
      const { objectId: messageGroupId } = await this.createObject(
        newMessageGroup, [{ key: this.EXTERNAL_ID_QUERY_HEADER_NAME, value: externalId }],
        [], this.TTL_TWO_DAYS,
      );
      this.logger.info('Created new message group');
      return {
        messageGroup: newMessageGroup, messageGroupSize, messageGroupId, isCreated: true,
      };
    }
    this.logger.info('MessageGroup found');
    const messageGroupId = messageGroups[0].objectId;
    const parsedMessageGroup = await this.lookupParsedObjectById(messageGroupId);
    return {
      messageGroup: parsedMessageGroup, messageGroupSize, messageGroupId, isCreated: false,
    };
  }

  async createNewObjectInMessageGroup(object, messageGroupId) {
    this.logger.info('Processing creation of the new object');
    const parsedMessageGroup = await this.lookupParsedObjectById(messageGroupId);
    this.logger.info('...Updating message group');
    parsedMessageGroup.messageIdsSeen[object.messageId] = object.messageId;
    return this.updateObject(messageGroupId, {
      ...parsedMessageGroup, messages: [...parsedMessageGroup.messages, object],
    });
  }
}

module.exports = ObjectStorageWrapperExtended;
