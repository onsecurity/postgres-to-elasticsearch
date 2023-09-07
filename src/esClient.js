const { Client: EsClient } = require('@elastic/elasticsearch');
const _ = require('lodash');
const moment = require('moment')

const config = require('../config');
const log = require('./log');

let existingEsIndices = [];
let creatingEsIndices = {};
let indexQueue = [];
let dataQueue = [];
let onFlushCallbacks = [];

const node = config.ES_PROTO + '://' + config.ES_HOST + ':' + config.ES_PORT
log.debug("Configured ES client ", node)
const client = new EsClient({
    node,
    auth: {
        username: config.ES_USERNAME,
        password: config.ES_PASSWORD
    },
    tls: {
        rejectUnauthorized: !config.ES_ALLOW_INSECURE_SSL
    }
});

const bulk = async function(data) {
    return new Promise(async (accept, reject) => {
        try {
            const response = await client.bulk({body: data})
            if (response.errors) {
                reject()
            } else {
                accept();
            }
        } catch (err) {
            log.info("Error running bulk operation")
            log.error(err)
        }
    })
};


const createIndexIfNotExists = async function(index) {
    if (!config.ES_PRE_CREATE_INDICIES) {
        return
    }
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
                    client.indices.create({
                        index,
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
    return new Promise(async (accept, reject) => {
        if (!dataQueue.length) {
            log.debug('Skipping flushQueue, no items');
            return accept();
        }
        if (dataQueue.length !==  indexQueue.length) {
            log.fatal('Error, dataQueue length does not match indexQueue length', dataQueue.length, indexQueue.length);
        }

        let indexQueueToPush = indexQueue.splice(0, indexQueue.length > config.QUEUE_LIMIT ? config.QUEUE_LIMIT : indexQueue.length);
        let dataQueueToPush = dataQueue.splice(0, dataQueue.length > config.QUEUE_LIMIT ? config.QUEUE_LIMIT : dataQueue.length);

        let uniqueIndexes = _.uniq(_.map(indexQueueToPush, item => item[config.ES_BULK_ACTION]._index));
        for (const uniqueIndex of uniqueIndexes) {
            await createIndexIfNotExists(uniqueIndex);
        }
        let bulkData = [];
        for (let i = 0; i < indexQueueToPush.length; i++) {
            bulkData.push(indexQueueToPush[i]);
            bulkData.push(dataQueueToPush[i]);
        }

        try {
            await bulk(bulkData)
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
};

const getEsIndex = function(tableName) {
    let index = '';
    if (config.ES_INDEX_PREFIX) {
        index += config.ES_INDEX_PREFIX
    }
    if (config.ES_INDEX_APPEND_TABLE_NAME) {
        index += '-' + tableName
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
        flushQueue().catch(() => {});
    }, config.QUEUE_TIMEOUT * 1000);
}

let readyPromise;

const queue = async function(data) {
    let index = getEsIndex(data.table_name);

    let indexRow = {[config.ES_BULK_ACTION]: {"_index": index}};
    indexQueue.push(indexRow);
    if (config.ES_LABEL_NAME !== null && config.ES_LABEL !== null) {
        data[config.ES_LABEL_NAME] = config.ES_LABEL;
    }
    dataQueue.push(data);
    if (indexQueue.length >= config.QUEUE_LIMIT) {
        await flushQueue().catch(() => {});
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
    getEsIndex,
    begin: beginInterval,
    clearInterval: () => clearInterval(flushInterval),
};