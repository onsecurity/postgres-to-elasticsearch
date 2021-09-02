const esClient = require('./esClient'),
    pgClient = require('./pgClient'),
    config = require('./config'),
    log = require('./log'),
    Cursor = require('pg-cursor'),
    PgEscape = require('pg-escape');

let getLastProcessedEventId = async function() {
    return new Promise(async (accept, reject) => {
        try {
            await esClient.ready();
            const tables = await pgClient.getAuditedTables()
            createPromises = tables.map(table => esClient.createIndexIfNotExists(esClient.getEsIndex(table)))
            await Promise.all(createPromises);
        } catch (err) {
            log.error(err.message)
            log.debug(err.stack)
            log.fatal("Failed initialising esClient")
        }
        let es = esClient.client();
        log.debug('Searching for last processed ' + config.PG_UID_COLUMN);
        let sort = {};
        sort[config.PG_TIMESTAMP_COLUMN] = {"order": "desc"};

        let searchBody = {
            from: 0,
            size: 1,
            sort: [sort]
        }

        if (config.ES_LABEL_NAME && config.ES_LABEL) {
            searchBody.query = {term: {[config.ES_LABEL_NAME + '.keyword']: config.ES_LABEL}};
        }
        log.debug('Searching for last processed ' + config.PG_UID_COLUMN + ' using searchBody:', searchBody);

        es.search({
            index: `${config.ES_INDEX_PREFIX}*`,
            body: searchBody,
        }).then((res) => {
            if (res.body.hits.total.value) {
                log.info('Found last processed ' + config.PG_UID_COLUMN + ': ' + res.body.hits.hits[0]._source[config.PG_UID_COLUMN]);
                accept(res.body.hits.hits[0]._source[config.PG_UID_COLUMN]);
            } else {
                log.info('No historic audit found, cannot get last processed ' + config.PG_UID_COLUMN);
                reject('No historic audit');
            }
        }).catch((err) => {
            log.fatal('Unable to get last processed event id', err);
        });
    });
};


let insertHistoricAudit = function(cursor) {
    let reader;
    let processedRows = 0;
    let highestEventId = null;

    reader = () => {
        cursor.read(config.QUEUE_LIMIT, (err, rows, result) => {
            if (err) {
                log.fatal('Unable to read historic audit cursor', err);
            }

            rows.forEach(async (row) => {
                if (row[config.PG_UID_COLUMN] > highestEventId || highestEventId === null) {
                    highestEventId = parseInt(row[config.PG_UID_COLUMN]);
                }
                esClient.queue(row);
            });

            processedRows += rows.length;

            if (!rows.length) {
                log.info('No more historic rows to process, processed a total of ' + processedRows + ' rows');
            } else {
                reader();
            }
        });
    };
    reader();
};

let deleteAfterHistoricAuditProcessed = async function(lastProcessedEventId) {
    return new Promise(async (accept, reject) => {
        if (config.PG_DELETE_ON_INDEX) {
            log.debug('Deleting rows where ' + config.PG_UID_COLUMN + ' <= ' + lastProcessedEventId);
            let pg = await pgClient.client();
            pg.query(
                'DELETE FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) +
                ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' <= $1',
                [lastProcessedEventId],
                (err, res) => {
                    if (err) {
                        log.error('Error when deleting rows from database', err);
                        return reject();
                    }
                    log.info('Deleted a total of ' + res.rowCount + ' rows');
                    return accept();
                }
            );
        } else {
            return accept();
        }
    });
};


let processHistoricAudit = async function() {
    return new Promise(async (accept, reject) => {
        log.info('Processing historic audit');
        let pg = await pgClient.client();
        getLastProcessedEventId().then((event_id) => {
            const cursor = pg.query(new Cursor('SELECT * FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) + ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' > $1', [event_id]));
            log.info('Historic audit query completed, processing...');
            insertHistoricAudit(cursor);
            deleteAfterHistoricAuditProcessed(event_id);
        }).catch(() => {
            log.info('Loading all available audit data for backlog processing');
            const cursor = pg.query(new Cursor('SELECT * FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE)));
            log.info('Historic audit query completed, processing...');
            insertHistoricAudit(cursor);
            return accept();
        });
    })
};

module.exports = {
    run: async function() {
        pgClient.client().then(async () => {
            await processHistoricAudit();
        });
    }
};


