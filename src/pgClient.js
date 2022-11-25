const _ = require('lodash');
const Pg = require('pg');
const PgEscape = require('pg-escape');
const config = require('../config');
const log = require('./log');
const esClient = require('./esClient');
let Hstore = require('pg-hstore');


let pgClients = {};
let connectToPg = async function(name = 'default') {
    if (pgClients[name]) {
        return pgClients[name];
    }
    pgClients[name] = new Promise((accept, reject) => {
        const pgClient = new Pg.Client({
            user: config.PG_USERNAME,
            host: config.PG_HOST,
            database: config.PG_DATABASE,
            password: config.PG_PASSWORD,
            port: config.PG_PORT,
            application_name: 'Eventful ' + name,
        });
        pgClient.connect(async (err) => {
            if (err) {
                log.fatal('Could not connect');
            } else {
                log.debug('PG connected');
                await setPgTypeParsers(pgClient);
                accept(pgClient);
            }
        });
    });
    return pgClients[name];
};

let setPgTypeParsers = async function(pgClient) {
    let types = Pg.types;
    let hstore = Hstore();

    await getHstoreTypeId(pgClient).then((typeId) => {
        log.info('Found Hstore type id, Hstore processing enabled.');
        types.setTypeParser(typeId, (val) => {
            return hstore.parse(val);
        });
    }).catch(() => {
        log.info('Cannot find hstore type id, will not process hstore data.');
    });
};

let getHstoreTypeId = async function(pgClient) {
    return new Promise(async (accept, reject) => {
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

let startListener = async function() {
    const pgClient = await connectToPg('listener');
    pgClient.on('notification', function (msg) {
        log.debug('Received notification on ' + msg.channel);
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

let loadPgRow = async function(event_id) {
    return new Promise(async (accept, reject) => {
        const pg = await connectToPg();
        pg.query('SELECT * FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) + ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' = $1', [event_id]).then((res) => {
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
    return new Promise(async (accept, reject) => {
        const pg = await connectToPg();
        pg.query(`SELECT table_name FROM ${PgEscape.ident(config.PG_SCHEMA)}.${PgEscape.ident(config.PG_TABLE)} GROUP BY table_name`)
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
    start: async function() {
        return startListener();
    },
    client: async function(name = 'default') {
        return connectToPg(name);
    },
    end: async function() {
        try {
            return Promise.all(Object.keys(pgClients).map(key => pgClients[key].end()));
        } catch(err) {
            log.error("Error while closing pgClient");
        }
    },
    getAuditedTables,
};