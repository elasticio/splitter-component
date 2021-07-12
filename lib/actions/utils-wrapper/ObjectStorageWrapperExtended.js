const { ObjectStorageWrapper } = require('@elastic.io/maester-client/dist/ObjectStorageWrapper');

class ObjectStorageWrapperExtended extends ObjectStorageWrapper {
  constructor(context) {
    super(context);
    this.logger = context.logger;
    this.EXTERNAL_ID_QUERY_HEADER_NAME = 'externalid';
    this.TTL_TWO_DAYS = 172800;
  }

  async lookupParsedObjectById(bucketId) {
    const bucket = await this.lookupObjectById(bucketId);
    return JSON.parse(bucket);
  }

  async createBucketIfNotExists(externalId, bucketSize) {
    this.logger.info('Processing creation of the new bucket');
    const buckets = await this.lookupObjectsByQueryParameters(
      [{ key: this.EXTERNAL_ID_QUERY_HEADER_NAME, value: externalId }],
    );
    if (buckets.length > 1) {
      throw new Error('Several buckets with the same ids can not exist');
    }
    if (!buckets.length) {
      this.logger.info('No buckets found');
      const newBucket = {
        messages: [],
        messageIdsSeen: {},
      };
      const { objectId: bucketId } = await this.createObject(
        newBucket, [{ key: this.EXTERNAL_ID_QUERY_HEADER_NAME, value: externalId }],
        [], this.TTL_TWO_DAYS,
      );
      this.logger.info('Created new bucket');
      return {
        bucket: newBucket, bucketSize, bucketId, isCreated: true,
      };
    }
    this.logger.info('Bucket found');
    const bucketId = buckets[0].objectId;
    const parsedBucket = await this.lookupParsedObjectById(bucketId);
    return {
      bucket: parsedBucket, bucketSize, bucketId, isCreated: false,
    };
  }

  async createNewObjectInBucket(object, bucketId) {
    this.logger.info('Processing creation of the new object');
    const parsedBucket = await this.lookupParsedObjectById(bucketId);
    this.logger.info('...Updating bucket');
    parsedBucket.messageIdsSeen[object.messageId] = object.messageId;
    return this.updateObject(bucketId, {
      ...parsedBucket, messages: [...parsedBucket.messages, object],
    });
  }
}

module.exports = ObjectStorageWrapperExtended;
