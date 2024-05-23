/* eslint-disable no-await-in-loop, no-restricted-syntax, guard-for-in, class-methods-use-this */
const { v4: uuidv4 } = require('uuid');
const { messages } = require('elasticio-node');
const { ObjectStorageWrapper } = require('@elastic.io/maester-client');
const { sleep } = require('@elastic.io/component-commons-library');
const packageJson = require('../package.json');
const compJson = require('../component.json');

const MAX_LOCAL_STORAGE_SIZE = 1024 * 1024 * 5;

const getGroupUniqueId = (groupId) => `${process.env.ELASTICIO_FLOW_ID}/${process.env.ELASTICIO_STEP_ID}/${groupId}`;
const getOriginalGroupId = (groupUniqueId) => groupUniqueId.replace(`${process.env.ELASTICIO_FLOW_ID}/${process.env.ELASTICIO_STEP_ID}/`, '');
const timestamp = (date) => new Date(date).getTime();

const getUserAgent = () => {
  const { name: compName } = packageJson;
  const { version: compVersion } = compJson;
  const maesterClientVersion = packageJson.dependencies['@elastic.io/maester-client'];
  return `${compName}/${compVersion} maester-client/${maesterClientVersion}`;
};

class GroupsProcessor {
  constructor(context, cfg) {
    this.cfg = cfg;
    this.logger = context.logger;
    this.context = context;
    this.groupsStorage = {};
    this.lastIteration = new Date();
    this.maester = new ObjectStorageWrapper(context, getUserAgent());
    this.statusCheckerRunning = false;
    this.debugRunning = false;
    this.lastStatusChecker = new Date();
    this.TTL_TWO_DAYS = 172800;
    this.maesterKey = 'externalid';
  }

  setContext(context) {
    this.logger = context.logger;
    this.context = context;
  }

  async addMessage(params) {
    this.lastIteration = new Date();
    const {
      groupId,
      messageData,
      groupSize,
      timersec,
      messageId = uuidv4(),
    } = params;
    const groupUniqueId = getGroupUniqueId(groupId);
    if (this.cfg.mode === 'groupSize' && !this.groupsStorage[groupUniqueId]?.wasSearchedInMaester) {
      this.logger.info(`Checking group "${groupId}" - if it exist in maester`);
      const existingGroups = await this.maester.lookupObjectsByQueryParameters(
        [{ key: this.maesterKey, value: groupUniqueId }],
      );

      if (existingGroups.length === 0) {
        this.logger.info(`Group "${groupId}" - doesn't exist in maester`);
        this.groupsStorage[groupUniqueId] ||= {};
      } else {
        this.logger.info(`Group "${groupId}" - exist in maester, extracting existing data`);
        const group = await this.maester.lookupObjectById(existingGroups[0].objectId);
        group.maesterId = existingGroups[0].objectId;
        this.groupsStorage[groupUniqueId] = group;
      }
      this.groupsStorage[groupUniqueId].wasSearchedInMaester = true;
    }
    this.groupsStorage[groupUniqueId] ||= {};
    const group = this.groupsStorage[groupUniqueId];
    group.messages ||= [];
    group.messages.push({ [messageId]: messageData || {} });
    group.readyAfter = timestamp(new Date()) + timersec;
    group.groupSize = groupSize;
    group.timersec = timersec;
    this.logger.info(`Message added to group "${groupId}", current size - ${group.messages.length}`);
    await this.checkAndEmitGroup(groupUniqueId);
    if (!this.statusCheckerRunning) {
      this.logger.info('Starting status checker');
      this.statusChecker();
    }
    if (!this.debugRunning) this.debug();
  }

  async checkAndEmitGroup(groupUniqueId) {
    const group = this.groupsStorage[groupUniqueId];
    const readyBySize = this.groupReadyBySize(group);
    const readyByTime = this.cfg.mode === 'groupSize' ? false : this.groupReadyByTime(group);
    const readyByOverload = JSON.stringify(this.groupsStorage).length > MAX_LOCAL_STORAGE_SIZE;
    if (readyBySize || readyByTime || readyByOverload) {
      const groupOriginalId = getOriginalGroupId(groupUniqueId);
      if (readyByOverload) {
        this.logger.warn(`Local storage is overloaded group "${groupOriginalId}" will be emitted to prevent data loss`);
      } else {
        this.logger.info(`Group "${groupOriginalId}" is ready by ${readyBySize ? 'Size' : 'Time'}`);
      }
      // eslint-disable-next-line no-undef
      const fixedGroup = structuredClone(this.groupsStorage[groupUniqueId]);
      delete group.messages;
      delete group.readyAfter;
      delete group.maesterId;

      await this.emitGroup(fixedGroup, groupOriginalId);
      this.logger.info(`Message with groupId: ${groupOriginalId} was emitted`);
      if (fixedGroup.maesterId) {
        this.logger.info(`Removing groupId ${groupOriginalId} from maester`);
        await this.maester.deleteObjectById(fixedGroup.maesterId);
      }
    }
  }

  async emitGroup(group, groupOriginalId) {
    let messageData;
    if (this.cfg.emitAsArray) {
      messageData = group.messages.map((msg) => Object.values(msg)[0]);
    } else {
      messageData = {};
      for (const message of group.messages) {
        Object.assign(messageData, message);
      }
    }
    await this.context.emit('data', messages.newMessageWithBody({
      groupSize: group.messages.length,
      groupId: groupOriginalId,
      messageData,
    }));
  }

  async emitAsIs() {
    for (const groupUniqueId in this.groupsStorage) {
      const groupOriginalId = getOriginalGroupId(groupUniqueId);
      await this.emitGroup(this.groupsStorage[groupUniqueId], groupOriginalId);
      delete this.groupsStorage[groupUniqueId];
    }
  }

  async checkAllGroups() {
    for (const groupUniqueId in this.groupsStorage) {
      await this.checkAndEmitGroup(groupUniqueId);
    }
  }

  groupReadyBySize(group) {
    const currentGroupSize = group.messages?.length || 0;
    return currentGroupSize >= group.groupSize;
  }

  groupReadyByTime(group) {
    return timestamp(new Date()) > group.readyAfter;
  }

  async statusChecker() {
    this.statusCheckerRunning = true;
    this.lastStatusChecker = new Date();
    if (this.cfg.mode !== 'groupSize') await this.checkAllGroups();
    if ((timestamp(new Date()) > timestamp(this.lastIteration) + 25000) && this.cfg.mode === 'groupSize') {
      this.logger.info('Going to save all unready groups to maester storage');
      for (const groupUniqueId in this.groupsStorage) {
        await this.saveGroupToMaester(groupUniqueId);
      }
      this.logger.info('All groups saved to maester, stopping status checker');
      this.statusCheckerRunning = false;
    } else {
      await sleep(10);
      await this.statusChecker();
    }
  }

  async debug() {
    this.debugRunning = true;
    // eslint-disable-next-line max-len
    this.logger.debug(`statusCheckerRunning: ${this.statusCheckerRunning}, now: ${timestamp(new Date())}, lastIteration: ${timestamp(this.lastIteration)} mode: ${this.cfg.mode}, ready to maester: ${(timestamp(new Date()) > timestamp(this.lastIteration) + 25000) && this.cfg.mode === 'groupSize'}, last: ${timestamp(this.lastStatusChecker)}`);
    await sleep(1000);
    await this.debug();
  }

  async saveGroupToMaester(groupUniqueId) {
    const group = this.groupsStorage[groupUniqueId];
    const groupOriginalId = getOriginalGroupId(groupUniqueId);
    let maesterId;
    if (group.maesterId) {
      maesterId = group.maesterId;
    } else {
      const existingGroups = await this.maester.lookupObjectsByQueryParameters(
        [{ key: this.maesterKey, value: groupUniqueId }],
      );
      maesterId = existingGroups[0]?.objectId;
    }

    if (!maesterId) {
      this.logger.info(`Group "${groupOriginalId} doesn't exist in maester, it will be created"`);
      group.maesterId = await this.maester.createObject(group, [{ key: this.maesterKey, value: groupUniqueId }], [], this.TTL_TWO_DAYS);
    } else {
      this.logger.info(`Group "${groupOriginalId} exist in maester, it will be updated"`);
      group.maesterId = maesterId;
      await this.maester.updateObjectById(maesterId, group);
    }
  }
}

module.exports.getGroupUniqueId = getGroupUniqueId;
module.exports.getOriginalGroupId = getOriginalGroupId;
module.exports.timestamp = timestamp;
module.exports.GroupsProcessor = GroupsProcessor;
