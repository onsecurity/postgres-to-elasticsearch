const _ = require('lodash');
const Pg = require('pg');
const PgEscape = require('pg-escape');
const config = require('../config');
const log = require('./log');
const esClient = require('./esClient');

const pgClient = new Pg.Client({
    user: config.PG_USERNAME,
    host: config.PG_HOST,
    database: config.PG_DATABASE,
    password: config.PG_PASSWORD,
    port: config.PG_PORT,
    connectionTimeoutMillis: 3e3
});

let connectPromise = null;
let connectToPg = function() {
    if (connectPromise === null) {
        connectPromise = new Promise((accept, reject) => {
            pgClient.connect((err) => {
                if (err) {
                    log.fatal('Could not connect');
                } else {
                    log.debug('PG connected');
                    setPgTypeParsers();
                    accept(pgClient);
                }
            });
        });
    }
    return connectPromise;
};

let setPgTypeParsers = function() {
    let types = Pg.types;
    let hstore = require('pg-hstore')();

    getHstoreTypeId().then((typeId) => {
        log.info('Found Hstore type id, Hstore processing enabled.');
        types.setTypeParser(typeId, (val) => {
            return hstore.parse(val);
        });
    }).catch(() => {
        log.info('Cannot find hstore type id, will not process hstore data.');
    });
};

let getHstoreTypeId = function() {
    return new Promise((accept, reject) => {
        pgClient.query(
            'SELECT oid FROM pg_type where typname = $1',
            ['hstore'],
            (err, res) => {
                if (err || !res.rows.length) {
                    log.debug('Hstore search error', err);
                    return reject();
                }

                if (res.rows.length) {
                    log.debug('Found hstore type oid', res.rows[0].oid);
                    return accept(res.rows[0].oid);
                }
            }
        );
    });
};

let startListener = function() {
    pgClient.on('notification', function (msg) {
        log.debug('Received notification on ' + msg.channel, msg.payload);
        if (msg.channel === config.PG_LISTEN_TO) {
            log.debug('Queuing item from ' + config.PG_LISTEN_TO);
            queueRecord(JSON.parse(msg.payload));
        } else if (msg.channel === config.PG_LISTEN_TO_ID) {
            log.debug('Queuing big item ' + msg.payload);
            loadPgRow(parseInt(msg.payload)).then((row) => {
                queueRecord(row);
            }).catch((err) => {
                log.error('Unable to load row from database, id: ', msg.payload);
                log.error(err);
            })
        }
    });
    log.debug('PG notification listener created');

    pgClient.query("LISTEN " + config.PG_LISTEN_TO).then(() => {
        log.info('LISTEN ' + config.PG_LISTEN_TO + ' statement completed');
    }).catch((err) => {
        log.fatal('LISTEN ' + config.PG_LISTEN_TO + ' statement failed');
    });
    pgClient.query("LISTEN " + config.PG_LISTEN_TO_ID).then(() => {
        log.info('LISTEN ' + config.PG_LISTEN_TO_ID + ' statement completed');
    }).catch((err) => {
        log.fatal('LISTEN ' + config.PG_LISTEN_TO_ID + ' statement failed', err);
    });
};

let loadPgRow = function(event_id) {
    return new Promise((accept, reject) => {
        pgClient.query('SELECT * FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) + ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' = $1', [event_id]).then((res) => {
            if (res.rowCount) {
                return accept(res.rows[0]);
            } else {
                return reject();
            }
        }).catch(() => {
            return reject();
        });
    });
};

let queueRecord = async function(row) {
    return esClient.queue(row);
};

let getAuditedTables = async function() {
    return new Promise((accept, reject) => {
        pgClient.query(`SELECT table_name FROM ${PgEscape.ident(config.PG_SCHEMA)}.${PgEscape.ident(config.PG_TABLE)} GROUP BY table_name`)
            .then(res => {
                if (res.rowCount) {
                    const tables = res.rows.map((row) => row.table_name)
                    return accept(tables);
                } else {
                    accept([]);
                }
            }).catch(() => {
                return reject();
            })
    })
}

module.exports = {
    start: function() {
        connectToPg().then(() => {
            startListener();
        })
    },
    client: async function() {
        return await connectToPg();
    },
    end: async function() {
        try {
            return await pgClient.end();
        } catch(err) {
            log.error("Error while closing pgClient");
        }
    },
    getAuditedTables,
};