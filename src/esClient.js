const config = require('./config');
const Es =  require('elasticsearch');
const log = require('./log');
const _ = require('lodash');

let existingEsIndices = [];
let creatingEsIndices = {};
let indexQueue = [];
let dataQueue = [];
let onFlushCallbacks = [];

const client = new Es.Client( {
    host: config.ES_PROTO + '://' + (!!config.ES_USERNAME && !!config.ES_PASSWORD ? config.ES_USERNAME + ':' + config.ES_PASSWORD + '@' : '') + config.ES_HOST + ':' + config.ES_PORT,
    log: config.DEBUG ? 'trace' : null,
    apiVersion: config.ES_VERSION
});

const bulk = async function(data) {
    return client.bulk({
        body: data
    });
};


const createIndexIfNotExists = async function(index) {
    if (creatingEsIndices[index]) {
        return creatingEsIndices[index];
    } else {
        creatingEsIndices[index] = new Promise((accept, reject) => {
            client.indices.exists({index}).then((exists) => {
                if (exists) {
                    if (existingEsIndices.indexOf(index) === -1) {
                        existingEsIndices.push(index);
                    }
                    return accept(index);
                } else {
                    let mappings = {};
                    if (parseInt(config.ES_VERSION.substr(0, 1)) >= 7) {
                        mappings = config.ES_MAPPING;
                    } else {
                        mappings[config.ES_TYPE] = config.ES_MAPPING;
                    }
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
            }).catch((err) => {
                log.fatal('indices.exists(' + index + ') failed.', err);
                return reject();
            });
        });
        return creatingEsIndices[index];
    }
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

        let uniqueIndexes = _.uniq(_.map(indexQueueToPush, item => item.index._index));
        for (let i = 0; i < uniqueIndexes.length; i++) {
            await createIndexIfNotExists(uniqueIndexes[i]);
        }
        let bulkData = [];
        for (let i = 0; i < indexQueueToPush.length; i++) {
            bulkData.push(indexQueueToPush[i]);
            bulkData.push(dataQueueToPush[i]);
        }
        bulk(bulkData).then(() => {
            log.debug('Successfully indexed ', dataQueueToPush.length, ' items');
            onFlushCallbacks.forEach((callback) => {
                callback(indexQueueToPush, dataQueueToPush)
            });
            return accept();
        }).catch((err) => {
            log.error('Failed to log ', dataQueueToPush.length, ' items', err);
            dataQueue = dataQueueToPush.concat(dataQueue);
            indexQueue = indexQueueToPush.concat(indexQueue);
            return reject();
        })
    });
};

const getEsIndex = function() {
    return config.ES_INDEX;
    // + (config.ES_INDEX_DATE_APPENDIX ? (new Date).toISOString().substr(0, 10) : '');
};

setInterval(() => {
    flushQueue().catch(() => {});
}, config.QUEUE_TIMEOUT * 1000);

let readyPromise;

module.exports = {
    ready: async function() {
        if (readyPromise === undefined) {
            readyPromise = client.ping({requestTimeout: 30000});
        }
        return readyPromise;
    },
    client: function() {
        return client;
    },
    queue: function(data) {
        let index = getEsIndex();
        let indexRow = {"index": {"_index": index}};
        if (parseInt(config.ES_VERSION.substr(0, 1)) < 7) {
            indexRow.index["_type"] = config.ES_TYPE;
        }
        indexQueue.push(indexRow);
        dataQueue.push(data);
        if (indexQueue.length >= config.QUEUE_LIMIT) {
            flushQueue().catch(() => {});
        }
    },
    createIndexIfNotExists: createIndexIfNotExists,
    flush: flushQueue,
    onFlush: function(callback) {
        onFlushCallbacks.push(callback);
    }
};