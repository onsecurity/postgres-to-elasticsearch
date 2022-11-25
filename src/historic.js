const esClient = require('./esClient'),
    pgClient = require('./pgClient'),
    config = require('../config'),
    log = require('./log'),
    Cursor = require('./cursor'),
    PgEscape = require('pg-escape');

const getIndexSearchTerm = () => {
    let index = '';
    if (config.ES_INDEX_PREFIX) {
        index += config.ES_INDEX_PREFIX + '-'
    }
    index += '*'
    log.debug(index)
    return index
}

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
        
        log.debug('Searching for last processed ' + config.PG_UID_COLUMN + ' using searchBody:', JSON.stringify(searchBody, null, 2));

        try {
            const searchResult = await es.search({
                index: getIndexSearchTerm(),
                body: searchBody,
            })
            if (searchResult.body.hits.total.value) {
                const hit = searchResult.body.hits.hits[0]
                log.info('Found last processed ' + config.PG_UID_COLUMN + ': ' + hit._source[config.PG_UID_COLUMN]);
                accept(hit._source[config.PG_UID_COLUMN]);
            } else {
                log.debug(searchResult.body);
                log.info('No historic audit found, cannot get last processed ' + config.PG_UID_COLUMN);
                reject('No historic audit');
            }
        } catch (err) {
            log.fatal('Unable to get last processed event id', err);
        }
    });
};


let insertHistoricAudit = async function(cursor) {
    let processedRows = 0;
    let highestEventId = null;
    // const originalPgDeleteOnIndex = config.PG_DELETE_ON_INDEX;
    // config.PG_DELETE_ON_INDEX = 0; // disable as this is deleted after in 1 bulk request
    let rows = await cursor.readAsync(config.QUEUE_LIMIT)
    if (rows.length) {
        while (rows.length) {
            for (const row of rows) {
                if (row[config.PG_UID_COLUMN] > highestEventId || highestEventId === null) {
                    highestEventId = parseInt(row[config.PG_UID_COLUMN]);
                }
                await esClient.queue(row);
            }
    
            processedRows += rows.length;
            log.info('Processed ' + processedRows + ' historic rows...');
            rows = await cursor.readAsync(config.QUEUE_LIMIT)
        }
        log.info('No more historic rows to process, processed a total of ' + processedRows + ' rows');
    } else {
        log.info('No historic rows to process');
    }
    // config.PG_DELETE_ON_INDEX = originalPgDeleteOnIndex;
    return highestEventId;
};

let deleteAfterHistoricAuditProcessed = async function(lastIndexedEventId, lastProcessedEventId) {
    return new Promise(async (accept, reject) => {
        if (config.PG_DELETE_ON_INDEX) {
            log.debug('Deleting rows where ' + config.PG_UID_COLUMN + ' > ' + lastIndexedEventId + ' and <= ' + lastProcessedEventId);
            let pg = await pgClient.client();
            pg.query(
                'DELETE FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) +
                ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' > $1' +
                ' AND ' + PgEscape.ident(config.PG_UID_COLUMN) + ' <= $2',
                [lastIndexedEventId, lastProcessedEventId],
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
        let lastEventIdIndexed = 0;
        let pg = await pgClient.client('historic');
        try {
            lastEventIdIndexed = await getLastProcessedEventId();
            const cursor = pg.query(new Cursor('SELECT * FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) + ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' > $1 ORDER BY ' + PgEscape.ident(config.PG_UID_COLUMN) + ' ASC', [lastEventIdIndexed]));
            log.info('Historic audit query completed, processing...');
            const lastProcessedEventId = await insertHistoricAudit(cursor);
            // await deleteAfterHistoricAuditProcessed(lastEventIdIndexed, lastProcessedEventId);
            return accept()
        } catch (err) {
            log.info('Loading all available audit data for backlog processing');
            const cursor = pg.query(new Cursor('SELECT * FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) + ' ORDER BY ' + PgEscape.ident(config.PG_UID_COLUMN) + ' ASC'));
            log.info('Historic audit query completed, processing...');
            const lastProcessedEventId = await insertHistoricAudit(cursor);
            // await deleteAfterHistoricAuditProcessed(lastEventIdIndexed, lastProcessedEventId);
            return accept();
        }
    })
};

module.exports = {
    run: async function() {
        try {
            await processHistoricAudit();
        } catch (err) {
            log.error("Error processing historic data");
        }
    }
};


