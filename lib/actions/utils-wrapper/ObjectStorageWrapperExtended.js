const { ObjectStorageWrapper } = require('@elastic.io/maester-client');

class ObjectStorageWrapperExtended extends ObjectStorageWrapper {
  constructor(context) {
    super(context);
    this.logger = context.logger;
    this.EXTERNAL_ID_QUERY_HEADER_NAME = 'externalid';
    this.TTL_TWO_DAYS = 172800;
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
      const messageGroupId = await this.createObject(
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
    const messageGroup = await this.lookupObjectById(messageGroupId);
    return {
      messageGroup, messageGroupSize, messageGroupId, isCreated: false,
    };
  }

  async createNewObjectInMessageGroup(object, messageGroupId) {
    this.logger.info('Processing creation of the new object');
    const messageGroup = await this.lookupObjectById(messageGroupId);
    this.logger.info('...Updating message group');
    messageGroup.messageIdsSeen[object.messageId] = object.messageId;
    return this.updateObjectById(messageGroupId, {
      ...messageGroup, messages: [...messageGroup.messages, object],
    });
  }
}

module.exports = ObjectStorageWrapperExtended;
