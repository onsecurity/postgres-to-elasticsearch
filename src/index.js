const DEBUG = process.env.DEBUG || false;
const INFO = DEBUG ? true : (process.env.INFO || true);
const PG_HOST = process.env.PG_HOST || 'localhost';
const PG_DATABASE = process.env.PG_DATABASE || 'databasename';
const PG_USERNAME = process.env.PG_USERNAME || 'root';
const PG_PASSWORD = process.env.PG_PASSWORD || '';
const PG_LISTEN_TO = process.env.PG_LISTEN_TO || 'audit';
const PG_PORT = process.env.PG_PORT || 5432
const ES_HOST = process.env.ES_HOST || 'localhost';
const ES_PORT = process.env.ES_PORT || 9200;
const ES_PROTO = process.env.ES_PROTO || 'https';
const ES_INDEX = process.env.ES_INDEX || 'audit';
const ES_TYPE = process.env.ES_TYPE || 'row';
const INDEX_QUEUE_LIMIT = process.env.QUEUE_LIMIT || 200;
const INDEX_QUEUE_TIMEOUT = process.env.QUEUE_TIMEOUT || 120;
const STATUS_UPDATE = DEBUG ? true : (process.env.STATUS_UPDATE || true);
const STATUS_UPDATE_INTERVAL = process.env.STATUS_UPDATE_INTERVAL || 60;

const Pg = require('pg');
const Es =  require('elasticsearch');
let addedIndexesTotal = 0;
let indexQueue = [];
let indexQueueTimeout = null;

const debug = function(...args) {
    if (DEBUG) {
        console.debug('DEBUG', ...args);
    }
};

const info = function(...args) {
    if (INFO) {
        console.info('INFO', ...args);
    }
}

const error = function(...args) {
    console.error('ERROR', ...args);
}

const fatal = function(...args) {
    console.error('FATAL',  ...args);
    flushIndexQueue();
    process.exit();
}

const pgClient = new Pg.Client({
    user: PG_USERNAME,
    host: PG_HOST,
    database: PG_DATABASE,
    password: PG_USERNAME,
    port: PG_PORT,
});

const esClient = new Es.Client( {
    hosts: [
        ES_PROTO + '://' + ES_HOST + ':' + ES_PORT,
    ]
});

let createAuditIndex = function() {
    return new Promise((accept, reject) => {
        esClient.indices.create({
            index: ES_INDEX,
        }, (err, resp, status) => {
            if (err) {
                fatal('Unable to create index', err);
            } else {
                info('Index ' + ES_INDEX + ' created successfully');
                accept();
            }
        });
    });
};

let startListener = function() {
    pgClient.connect((err) => {
        if (err) {
            fatal('Could not connect');
        } else {
            debug('PG connected');
            pgClient.on('notification', function (msg) {
                debug('Received notification', msg);
                createRecord(msg.payload);
            });
            debug('PG notification listener created');

            let listenQuery = pgClient.query("LISTEN " + PG_LISTEN_TO).then(() => {
                info('LISTEN statement completed');
            }).catch(() => {
                fatal('LISTEN statement failed');
            });
        }
    });
};

let createQueue = null;
let createRecord = function(payload) {
    debug('Queuing record with payload: ', payload);

    indexQueue.push({
        index: {
            _index: ES_INDEX,
            _type: ES_TYPE
        }
    }, {
        payload
    });

    debug('New indexQueue length: ' + (indexQueue.length / 2));

    if (indexQueue.length / 2 >= INDEX_QUEUE_LIMIT) {
        debug('indexQueue length is ' + (indexQueue.length / 2) + ', flushing as over INDEX_QUEUE_LIMIT');
        flushIndexQueue();
    } else {
        startFlushIndexQueueTimeout();
    }
};

let startFlushIndexQueueTimeout = function() {
    if (indexQueueTimeout !== null) {
        debug('Not starting indexQueueTimeout, already started');
        return;
    }

    debug('Starting indexQueueTimeout');
    indexQueueTimeout = setTimeout(() => {
        debug('indexQueueTimeout running');
        flushIndexQueue();
    }, INDEX_QUEUE_TIMEOUT * 1000);
};

let flushIndexQueue = function() {
    return new Promise((accept, reject) => {
        if (!indexQueue.length) {
            debug('Skipping flushIndexQueue, no items to process');
            return accept();
        }

        let flushingIndexQueue = indexQueue.slice(0);
        indexQueue = [];
        let indexQueueLength = flushingIndexQueue.length / 2;
        debug('Flushing index queue of ' + indexQueueLength + ' items');
        esClient.bulk({
            body: flushingIndexQueue
        }, (err, resp, status) => {
            if (err) {
                error(err, resp);
                debug('Readding ' + indexQueueLength + ' items to the indexQueue');
                indexQueue = flushingIndexQueue.concat(indexQueue);
                debug('New indexQueue length: ' + (indexQueue.length / 2));
                reject();
            } else {
                debug('Successfully created ' + indexQueueLength + ' indices', resp);
                debug('New indexQueue length: ' + (indexQueue.length / 2));
                addedIndexesTotal += indexQueueLength;
                accept();
            }
        });
    });
};

info('Starting');
debug('Checking for ES Index');
esClient.indices.get({index: ES_INDEX}).then((a) => {
    debug('ES Index found, starting listener');
    startListener()
}).catch((err) => {
    if (err.status === 404) {
        info('Index ' + ES_INDEX + ' does not exist, creating.');
        createAuditIndex().then(() => {
            startListener();
        }).catch((err) => {
            fatal('Failed to created index ' + ES_INDEX + ' error: ', err);
        })
    } else {
        fatal('Unable to start, index does not exist or error when creating', err);
    }
});

if (STATUS_UPDATE) {
    debug('Starting STATUS_UPDATE interval with interval of ' + (STATUS_UPDATE_INTERVAL * 60 * 1000) + ' seconds');
    setInterval(() => {
        console.log('STATUS UPDATE: ' + 'Created a total of ' + addedIndexesTotal + ' indexes with ' + (indexQueue.length / 2)  + ' queued at ' + parseInt(new Date().getTime() / 1000));
    }, STATUS_UPDATE_INTERVAL * 60 * 1000);
}

process.on('SIGTERM', function () {
    debug('Received SIGTERM');
    debug('Closing PG connection')
    pgClient.end().then(() => {
        debug('Closed PG connection');
        debug('Flushing indexQueue');
        flushIndexQueue().then(() => {
            debug('Flushed indexQueue');
            debug('Closing ES connection');
            esClient.close();
            debug('Closed ES connection');
            debug('Exiting');
            process.exit(0);
        }).catch(() => {
            error('Unable to flush queue, unable to quit gracefully')
        });
    }).catch(() => {
        error('Unable to close PG connection, unable to quit gracefully')
    })
});
