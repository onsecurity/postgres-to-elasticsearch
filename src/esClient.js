const { Client: OsClient } = require('@opensearch-project/opensearch');
const _ = require('lodash');
const moment = require('moment')
const config = require('../config');
const log = require('./log');
const PQueue = import('p-queue');

let bulkQueue = null;

let existingEsIndices = [];
let creatingEsIndices = {};
let indexQueue = [];
let dataQueue = [];
let onFlushCallbacks = [];
let currentFlushPromise = null;

const client = new OsClient({
    node: config.ES_PROTO + '://' + config.ES_HOST + ':' + config.ES_PORT,
    auth: {
        username: config.ES_USERNAME,
        password: config.ES_PASSWORD
    }
});

const getBulkQueue = async function () {
    if (bulkQueue === null) {
        let PQueueConstructor = (await PQueue).default;
        bulkQueue = new PQueueConstructor({
            concurrency: 1,
            throwOnTimeout: true,
            intervalCap: 5, // 5 per 10 seconds
            interval: 10000
        });
    }
    return bulkQueue;
}

const bulk = async function(data) {
    return new Promise(async (accept, reject) => {
        try {
            const response = await (await getBulkQueue()).add(() => client.bulk({body: data}));
            if (response.body.errors) {
                log.error("Error running bulk operation", response.body.errors)
                reject(response.body.errors)
            } else {
                accept();
            }
        } catch (err) {
            log.error("Error running bulk operation", err)
            reject();
        }
    })
};


const createIndexIfNotExists = async function(index) {
    if (!creatingEsIndices[index]) {
        creatingEsIndices[index] = new Promise(async (accept, reject) => {
            try {
                const { body } = await client.indices.exists({index})
                if (body) {
                    if (existingEsIndices.indexOf(index) === -1) {
                        existingEsIndices.push(index);
                    }
                    return accept(index);
                } else {
                    let mappings = {"properties": {}};
                    mappings.properties[config.PG_TIMESTAMP_COLUMN] = {"type": "date"};
                    mappings.properties[config.PG_UID_COLUMN] = {"type": "long"};
                    mappings.properties[config.ES_LABEL_NAME] = {"type": "keyword"};

                    client.indices.create({
                        index,
                        "body": {mappings}
                    }).then(() => {
                        if (existingEsIndices.indexOf(index) === -1) {
                            existingEsIndices.push(index);
                        }
                        return accept(index);
                    }).catch((err) => {
                        log.fatal('indices.create(' + index + ') failed.', err);
                        return reject();
                    });
                }
            } catch (err) {
                log.fatal('indices.exists(' + index + ') failed.', err);
                return reject();
            }
        });
    }
    return creatingEsIndices[index];
};

const flushQueue = async function() {
    currentFlushPromise = new Promise(async (accept, reject) => {
        if (!dataQueue.length) {
            log.debug('Skipping flushQueue, no items');
            return accept();
        }
        if (dataQueue.length !==  indexQueue.length) {
            log.fatal('Error, dataQueue length does not match indexQueue length', dataQueue.length, indexQueue.length);
        }

        let indexQueueToPush = indexQueue.splice(0, indexQueue.length > config.QUEUE_LIMIT ? config.QUEUE_LIMIT : indexQueue.length);
        let dataQueueToPush = dataQueue.splice(0, dataQueue.length > config.QUEUE_LIMIT ? config.QUEUE_LIMIT : dataQueue.length);

        let uniqueIndexes = _.uniq(_.map(indexQueueToPush, item => item.index._index));
        for (const uniqueIndex of uniqueIndexes) {
            await createIndexIfNotExists(uniqueIndex);
        }
        let bulkData = [];
        for (let i = 0; i < indexQueueToPush.length; i++) {
            bulkData.push(indexQueueToPush[i]);
            bulkData.push(dataQueueToPush[i]);
        }

        try {
            if (config.ES_MAX_POST_SIZE && JSON.stringify(bulkData).length > config.ES_MAX_POST_SIZE) {
                log.info('Bulk post size is too large, splitting into chunks');
                for (let chunk of _.chunk(bulkData, 2)) {
                    await bulk(chunk);
                }
            } else {
                await bulk(bulkData);
            }
            log.debug('Successfully indexed ', dataQueueToPush.length, ' items');
            for (const callback of onFlushCallbacks) {
                await callback(indexQueueToPush, dataQueueToPush).catch(err => log.error(err))
            }
            return accept();
        } catch (err) {
            log.error('Failed to log ', dataQueueToPush.length, ' items', err);
            dataQueue = dataQueueToPush.concat(dataQueue);
            indexQueue = indexQueueToPush.concat(indexQueue);
            return reject();
        }
    });
    return currentFlushPromise;
};

const getEsIndex = function(tableName) {
    let index = '';
    if (config.ES_INDEX_PREFIX) {
        index += config.ES_INDEX_PREFIX + '-'
    }
    index += tableName
    if (config.ES_INDEX_DATE_SUFFIX_FORMAT) {
        const dateString = moment().format(config.ES_INDEX_DATE_SUFFIX_FORMAT)
        index += '-' + dateString
    }
    return index
};

let flushInterval
const beginInterval = () => {
    flushInterval = setInterval(() => {
        log.debug("Flushing at interval");
        flushQueue();
    }, config.QUEUE_TIMEOUT * 1000);
}

let readyPromise;

const queue = async function(data) {
    let index = getEsIndex(data.table_name);

    let indexRow = {"index": {"_index": index}};
    indexQueue.push(indexRow);
    if (config.ES_LABEL_NAME !== null && config.ES_LABEL !== null) {
        data[config.ES_LABEL_NAME] = config.ES_LABEL;
    }
    dataQueue.push(data);
    if (indexQueue.length >= config.QUEUE_LIMIT || (config.ES_MAX_POST_SIZE && JSON.stringify(dataQueue).length > config.ES_MAX_POST_SIZE)) {
        await flushQueue();
    }
}

module.exports = {
    ready: async function() {
        if (readyPromise === undefined) {
            readyPromise = client.ping({}, {requestTimeout: 30000});
        }
        return readyPromise;
    },
    client: function() {
        return client;
    },
    queue: queue,
    createIndexIfNotExists: createIndexIfNotExists,
    flush: flushQueue,
    onFlush: function(callback) {
        onFlushCallbacks.push(callback);
    },
    waitForFlush: async function() {
        return new Promise(accept => currentFlushPromise === null ? accept() : currentFlushPromise.then(() => accept()));
    },
    getEsIndex,
    begin: beginInterval,
    clearInterval: () => clearInterval(flushInterval),
};