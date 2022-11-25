const _ = require('lodash'),
    config = require('./config'),
    esClient = require('./src/esClient'),
    pgClient = require('./src/pgClient'),
    historic = require('./src/historic'),
    PgEscape = require('pg-escape'),
    log = require('./src/log');


esClient.onFlush(async function(indexQueue, dataQueue) {
    return new Promise(async (accept, reject) => {
        if (config.PG_DELETE_ON_INDEX) {
            let pg = await pgClient.client('delete');
            let eventIds = _.map(dataQueue, index => index[config.PG_UID_COLUMN]);
            log.debug('UIDs to be deleted: ', eventIds);
            log.debug('Deleting ' + eventIds.length + ' rows after index');
            let chunks = _.chunk(eventIds, 10000);
            log.debug('Chunked ' + eventIds.length + ' rows into ' + chunks.length + ' chunks.');
            for (const [index, chunk] of chunks.entries()) {
                let params = [];
                for (let paramId = 1; paramId <= chunk.length; paramId++) {
                    params.push('$' + paramId);
                }
                try {
                    log.debug('Attempting to delete ' + chunk.length + ' rows...');
                    const deleteQuery = {
                        text: 'DELETE FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) +
                            ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' IN (' + params.join(',') + ')',
                        values: chunk
                    }
                    const { rowCount } = await pg.query(deleteQuery);
                    log.debug('Attempted to delete ' + chunk.length + ' rows, actually deleted ' + rowCount + ' rows');
                } catch(err) {
                    log.error('Error when deleting rows from database', err);
                    reject();
                    return;
                }
            }
            accept();
        } else {
            return accept();
        }
    });
});

const exit = async () => {
    log.log('Received SIGTERM, shutting down');
    log.log('Flushing remaining queue');
    esClient.clearInterval();
    try {
        await esClient.flush()
        log.debug('Flushed indexQueue');

        await esClient.waitForFlush();
    
        log.debug('Closing PG connection');
        await pgClient.end()
        log.debug('Closed PG connection');
        
        log.log('Exiting gracefully');
        process.exit(0);
    } catch (err) {
        log.error('Unable to quit gracefully')
    }
}

process.on('SIGTERM', exit);
process.on('SIGINT', exit);

historic.run()
    .then(() => {
        pgClient.start();
        esClient.begin()
    })
    .catch(err => {
        log.fatal("Error processing historic")
    });