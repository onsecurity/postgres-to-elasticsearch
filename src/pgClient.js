const _ = require('lodash');
const Pg = require('pg');
const PgEscape = require('pg-escape');
const config = require('../config');
const log = require('./log');
const esClient = require('./esClient');

const pool = new Pg.Pool({
    user: config.PG_USERNAME,
    host: config.PG_HOST,
    database: config.PG_DATABASE,
    password: config.PG_PASSWORD,
    port: config.PG_PORT,
    connectionTimeoutMillis: 3e3,
    ...(config.PG_SSL ? {
        ssl: {
            ca: config.PG_SSL_CA,
        }
    } : {})
});

let hstoreParserSet = false
let listenerClient;

const init = async function() {
    if (!hstoreParserSet) {
        await setPgTypeParsers();
        hstoreParserSet = true
    }
}

const query = async (text, params) => {
    return pool.query(text, params)
}

const setPgTypeParsers = async function() {
    let types = Pg.types;
    let hstore = require('pg-hstore')();

    try {
        const typeId = await getHstoreTypeId()
        log.info('Found Hstore type id, Hstore processing enabled.');
        types.setTypeParser(typeId, (val) => {
            return hstore.parse(val);
        });
    } catch (err) {
        log.info('Cannot find hstore type id, will not process hstore data.');
    }
};

const getHstoreTypeId = async function() {
    try {
        const result = await pool.query('SELECT oid FROM pg_type where typname = $1', ['hstore']);
        if (result.rows.length) {
            log.debug('Found hstore type oid', result.rows[0].oid);
            return result.rows[0].oid;
        }
    } catch (err) {
        log.debug('Hstore search error', err);
        throw err
    }
};

let startListener = async function() {
    const client = await pool.connect()
    listenerClient = client
    client.on('notification', function (msg) {
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

    client.query("LISTEN " + config.PG_LISTEN_TO).then(() => {
        log.info('LISTEN ' + config.PG_LISTEN_TO + ' statement completed');
    }).catch((err) => {
        log.fatal('LISTEN ' + config.PG_LISTEN_TO + ' statement failed');
    });
    client.query("LISTEN " + config.PG_LISTEN_TO_ID).then(() => {
        log.info('LISTEN ' + config.PG_LISTEN_TO_ID + ' statement completed');
    }).catch((err) => {
        log.fatal('LISTEN ' + config.PG_LISTEN_TO_ID + ' statement failed', err);
    });
};

let loadPgRow = function(event_id) {
    return new Promise((accept, reject) => {
        query('SELECT * FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) + ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' = $1', [event_id]).then((res) => {
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
        query(`SELECT table_name FROM ${PgEscape.ident(config.PG_SCHEMA)}.${PgEscape.ident(config.PG_TABLE)} GROUP BY table_name`)
            .then(res => {
                if (res.rowCount) {
                    const tables = res.rows.map((row) => row.table_name)
                    return accept(tables);
                } else {
                    accept([]);
                }
            }).catch((err) => {
                return reject(err);
            })
    })
}

module.exports = {
    init,
    start: startListener,
    query,
    getPool: function() {
        return pool
    },
    end: async function() {
        try {
            listenerClient && await listenerClient.end()
            return await pool.end();
        } catch(err) {
            log.error("Error while closing pgClient");
        }
    },
    getAuditedTables,
};