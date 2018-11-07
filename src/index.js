const DEBUG = process.env.DEBUG || false; // Show debug + info messages
const INFO = DEBUG ? true : (process.env.INFO || true); // Show info messages
const PG_HOST = process.env.PG_HOST || 'localhost'; // Host of the PostgreSQL database server
const PG_DATABASE = process.env.PG_DATABASE || 'databasename'; // Name of the PostgreSQL database
const PG_USERNAME = process.env.PG_USERNAME || 'root'; // Username of the PostgreSQL database
const PG_PASSWORD = process.env.PG_PASSWORD || ''; // Password of the PostgreSQL database
const PG_LISTEN_TO = process.env.PG_LISTEN_TO || 'audit'; // The LISTEN queue to listen on for the PostgreSQL database - https://www.postgresql.org/docs/9.1/static/sql-notify.html
const PG_PORT = process.env.PG_PORT || 5432; // The port that the PostgreSQL database is listening on
const PG_TIMESTAMP_COLUMN = process.env.PG_TIMESTAMP_COLUMN || 'action_timestamp'; // The column of the row that is used as the timestamp for Elasticsearch
const PG_UID_COLUMN = process.env.PG_UID_COLUMN || 'event_id'; // The primary key column of the row that is stored in Elasticsearch
const PG_DELETE_ON_INDEX = process.env.PG_DELETE_ON_INDEX || false; // Delete the rows in PostgreSQL after they have been indexed to Elasticsearch
const PG_DELETE_SCHEMA = process.env.PG_DELETE_SCHEMA || 'public'; // The schema of the table which rows which will be deleted from PG_DELETE_ON_INDEX
const PG_DELETE_TABLE = process.env.PG_DELETE_TABLE || 'audit'; // The table name which rows will be deleted from by PG_DELETE_ON_INDEX
const ES_LABEL_NAME = process.env.ES_LABEL_NAME || 'label';
const ES_LABEL = process.env.ES_LABEL || null;
const ES_HOST = process.env.ES_HOST || 'localhost'; // The hostname for the Elasticsearch server (pooling not supported currently)
const ES_PORT = process.env.ES_PORT || 9200;  // The port for the Elasticsearch server
const ES_PROTO = process.env.ES_PROTO || 'https'; // The protocol used for the Elasticsearch server connections
const ES_INDEX = process.env.ES_INDEX || 'audit'; // The Elasticsearch index the data should be stored in
const ES_TYPE = process.env.ES_TYPE || 'row'; // The type of the data to be stored in Elasticsearch
const INDEX_QUEUE_LIMIT = process.env.INDEX_QUEUE_LIMIT || 200; // The maximum number of items that should be queued before pushing to Elasticsearch
const INDEX_QUEUE_TIMEOUT = process.env.INDEX_QUEUE_TIMEOUT || 120; // The maximum seconds for an item to be in the queue before it is pushed to Elasticsearch
const STATUS_UPDATE = DEBUG ? true : (process.env.STATUS_UPDATE || true); // Show a status update message in stdout
const STATUS_UPDATE_INTERVAL = process.env.STATUS_UPDATE_INTERVAL || 60; // How often (in seconds) the status update message should be sent

const _ = require('lodash');
const Pg = require('pg');
const Cursor = require('pg-cursor');
const PgEscape = require('pg-escape');
const Es =  require('elasticsearch');

let addedIndexesTotal = 0;
let indexQueue = [];
let indexQueueTimeout = null;

const log = function(...args) {
    console.log('LOG', ...args);
};

const debug = function(...args) {
    if (DEBUG) {
        console.debug('DEBUG', ...args);
    }
};

const info = function(...args) {
    if (INFO) {
        console.info('INFO', ...args);
    }
};

const error = function(...args) {
    console.error('ERROR', ...args);
};

const fatal = function(...args) {
    console.error('FATAL',  ...args);
    flushIndexQueue();
    process.exit();
};

const pgClient = new Pg.Client({
    user: PG_USERNAME,
    host: PG_HOST,
    database: PG_DATABASE,
    password: PG_PASSWORD,
    port: PG_PORT,
});

const esClient = new Es.Client( {
    hosts: [
        ES_PROTO + '://' + ES_HOST + ':' + ES_PORT,
    ]
});

let createAuditIndex = function() {
    return new Promise((accept, reject) => {
        let mappings = {}
        mappings[ES_TYPE] = {"properties": {}};
        mappings[ES_TYPE].properties[PG_TIMESTAMP_COLUMN] = {"type": "date"};
        mappings[ES_TYPE].properties[PG_UID_COLUMN] = {"type": "long"};
        mappings[ES_TYPE].properties[ES_LABEL_NAME] = {"type": "keyword"};

        esClient.indices.create({
            index: ES_INDEX,
            "body": {mappings}
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
    pgClient.on('notification', function (msg) {
        debug('Received notification', msg);
        createRecord(JSON.parse(msg.payload));
    });
    debug('PG notification listener created');

    let listenQuery = pgClient.query("LISTEN " + PG_LISTEN_TO).then(() => {
        info('LISTEN statement completed');
    }).catch(() => {
        fatal('LISTEN statement failed');
    });
};

let processHistoricAudit = function() {
    info('Processing historic audit');
    getLastProcessedEventId().then((event_id) => {
        const cursor = pgClient.query(new Cursor('SELECT * FROM audit.logged_actions WHERE ' + PgEscape.ident(PG_UID_COLUMN) + ' > $1', [event_id]));
        info('Historic audit query completed, processing...');
        insertHistoricAudit(cursor);
        if (PG_DELETE_ON_INDEX) {
            deleteAfterHistoricAuditProcessed(event_id);
        }
    }).catch(() => {
        info('Loading all available audit data for backlog processing');
        const cursor = pgClient.query(new Cursor('SELECT * FROM audit.logged_actions'));
        info('Historic audit query completed, processing...');
        insertHistoricAudit(cursor);
    });
};

let insertHistoricAudit = function(cursor) {
    let reader;
    let processedRows = 0;
    let hstore = require('pg-hstore')();
    let highestEventId = null;

    reader = () => {
        cursor.read(INDEX_QUEUE_LIMIT, (err, rows, result) => {
            if (err) {
                fatal('Unable to read historic audit cursor', err);
            }

            rows.forEach(async (row) => {
                if (row[PG_UID_COLUMN] > highestEventId || highestEventId === null) {
                    highestEventId = parseInt(row[PG_UID_COLUMN]);
                }
                await createRecord(row);
            });

            processedRows += rows.length;

            if (!rows.length) {
                info('No more historic rows to process, processed a total of ' + processedRows + ' rows');
                if (PG_DELETE_ON_INDEX && highestEventId !== null) {
                    deleteAfterHistoricAuditProcessed(highestEventId);
                }
            } else {
                reader();
            }
        });
    };
    reader();
};

let getLastProcessedEventId = function() {
    return new Promise((accept, reject) => {
        debug('Searching for last processed ' + PG_UID_COLUMN);
        let sort = {};
        sort[PG_TIMESTAMP_COLUMN] = {"order": "desc"};

        let query = {match_all: {}};
        if (ES_LABEL_NAME && ES_LABEL) {
            query = {term: {}};
            query.term[ES_LABEL_NAME] = ES_LABEL;
        }
        debug('Searching for last processed ' + PG_UID_COLUMN + ' using query:', query);

        esClient.search({
            index: ES_INDEX,
            body: {
                query,
                from: 0,
                size: 1,
                sort: [sort]
            },
        }).then((res) => {
            if (res.hits.total) {
                info('Found last processed ' + PG_UID_COLUMN + ': ' + res.hits.hits[0]._source[PG_UID_COLUMN]);
                accept(res.hits.hits[0]._source[PG_UID_COLUMN]);
            } else {
                info('No historic audit found, cannot get last processed ' + PG_UID_COLUMN);
                reject('No historic audit');
            }
        }).catch((err) => {
            fatal('Unable to get last processed event id', err);
        });
    });
};

let createRecord = async function(payload) {
    return new Promise((accept, reject) => {
        debug('Queuing record with payload: ', payload);

        let postPayload = {...payload};

        if (ES_LABEL && ES_LABEL_NAME) {
            postPayload[ES_LABEL_NAME] = ES_LABEL;
        }

        indexQueue.push(
            {
                index: {
                    _index: ES_INDEX,
                    _type: ES_TYPE
                }
            },
            postPayload
        );

        debug('New indexQueue length: ' + (indexQueue.length / 2));

        if (indexQueue.length / 2 >= INDEX_QUEUE_LIMIT) {
            debug('indexQueue length is ' + (indexQueue.length / 2) + ', flushing as over INDEX_QUEUE_LIMIT');
            flushIndexQueue().then(() => {
                accept();
            }).catch(() => {
                //
            })
        } else {
            startFlushIndexQueueTimeout();
            accept();
        }
    });
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
        indexQueueTimeout = null;
    }, INDEX_QUEUE_TIMEOUT * 1000);
};

let deleteAfterIndex = function(event_ids) {
    return new Promise((accept, reject) => {
        debug('Deleting ' + event_ids.length + ' rows after index');
        let chunks = _.chunk(event_ids, 34464);
        debug('Chunked ' + event_ids.length + ' rows into ' + chunks.length + ' chunks.');
        chunks.forEach((chunk, index) => {

            let params = [];
            for (let paramId = 1; paramId <= chunk.length; paramId++) {
                params.push('$' + paramId);
            }

            let result = pgClient.query(
                'DELETE FROM ' + PgEscape.ident(PG_DELETE_SCHEMA) + '.' + PgEscape.ident(PG_DELETE_TABLE) +
                ' WHERE ' + PgEscape.ident(PG_UID_COLUMN) + ' IN (' + params.join(',') + ')',
                chunk,
                (err, res) => {
                    if (err) {
                        error('Error when deleting rows from database', err);
                        reject();
                        return;
                    }
                    info('Attempted to delete ' + chunk.length + ' rows, actually deleted ' + res.rowCount + ' rows');
                    if (index === chunks.length - 1) {
                        accept();
                    }
                }
            );
        });
    });
};

let deleteAfterHistoricAuditProcessed = function(lastProcessedEventId) {
    debug('Deleting rows where ' + PG_UID_COLUMN + ' <= ' + lastProcessedEventId);
    let result = pgClient.query(
        'DELETE FROM ' + PgEscape.ident(PG_DELETE_SCHEMA) + '.' + PgEscape.ident(PG_DELETE_TABLE) +
        ' WHERE ' + PgEscape.ident(PG_UID_COLUMN) + ' <= $1',
        [lastProcessedEventId],
        (err, res) => {
            if (err) {
                error('Error when deleting rows from database', err);
                return;
            }
            info('Deleted a total of ' + res.rowCount + ' rows');
        }
    );
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
        debug('Queue items to be flushed', flushingIndexQueue);
        esClient.bulk({
            body: flushingIndexQueue
        }, (err, resp, status) => {
            if (err) {
                error('Unable to flush queue', err, resp);
                debug('Readding ' + indexQueueLength + ' items to the indexQueue');
                indexQueue = flushingIndexQueue.concat(indexQueue);
                debug('New indexQueue length: ' + (indexQueue.length / 2));
                reject();
            } else {
                info('Flushed ' + indexQueueLength + ' records');
                debug('Successfully created ' + indexQueueLength + ' indices', resp);
                debug('New indexQueue length: ' + (indexQueue.length / 2));
                addedIndexesTotal += indexQueueLength;
                if (PG_DELETE_ON_INDEX) {
                    let eventIds = _.map(_.filter(flushingIndexQueue, (val, index) => index % 2 === 1), (index) => index[PG_UID_COLUMN]);
                    debug('UIDs to be deleted: ', eventIds);
                    deleteAfterIndex(eventIds).then(() => {
                        accept();
                    });
                } else {
                    accept();
                }
            }
        });
    });
};

let connectToPg = function() {
    return new Promise((accept, reject) => {
        pgClient.connect((err) => {
            if (err) {
                fatal('Could not connect');
            } else {
                debug('PG connected');
                setPgTypeParsers();
                accept();
            }
        });
    });
};

let setPgTypeParsers = function() {
    let types = Pg.types;
    let hstore = require('pg-hstore')();

    getHstoreTypeId().then((typeId) => {
        info('Found Hstore type id, Hstore processing enabled.');
        types.setTypeParser(typeId, (val) => {
            return hstore.parse(val);
        });
    }).catch(() => {
        info('Cannot find hstore type id, will not process hstore data.');
    });
};

let getHstoreTypeId = function() {
    return new Promise((accept, reject) => {
        let result = pgClient.query(
            'SELECT oid FROM pg_type where typname = $1',
            ['hstore'],
            (err, res) => {
                if (err || !res.rows.length) {
                    debug('Hstore search error', err);
                    return reject();
                }

                if (res.rows.length) {
                    debug('Found hstore type oid', res.rows[0].oid);
                    return accept(res.rows[0].oid);
                }
            }
        );
    });
};

let start = function() {
    connectToPg().then(() => {
        startListener()
        processHistoricAudit();
    })
};

info('Starting');
debug('Checking for ES Index');
esClient.indices.get({index: ES_INDEX}).then((a) => {
    debug('ES Index found, starting');
    start();
}).catch((err) => {
    if (err.status === 404) {
        info('Index ' + ES_INDEX + ' does not exist, creating.');
        createAuditIndex().then(() => {
            start();
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
    log('Received SIGTERM, shutting down');
    log('Flushing remaining queue');
    flushIndexQueue().then(() => {
        debug('Flushed indexQueue');
        debug('Closing PG connection');
        pgClient.end().then(() => {
            debug('Closed PG connection');
            debug('Closing ES connection');
            esClient.close();
            debug('Closed ES connection');
            log('Exiting gracefully');
            process.exit(0);
        }).catch(() => {
            error('Unable to flush queue, unable to quit gracefully')
        });
    }).catch(() => {
        error('Unable to close PG connection, unable to quit gracefully')
    })
});
