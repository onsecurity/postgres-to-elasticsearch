const _ = require('lodash'),
    config = require('./config'),
    esClient = require('./src/esClient'),
    pgClient = require('./src/pgClient'),
    historic = require('./src/historic'),
    PgEscape = require('pg-escape'),
    log = require('./src/log');


esClient.onFlush(async function(indexQueue, dataQueue) {
    if (config.PG_DELETE_ON_INDEX) {
        let eventIds = dataQueue.map(index => index[config.PG_UID_COLUMN]);
        log.debug('UIDs to be deleted: ', eventIds);
        log.debug('Deleting ' + eventIds.length + ' rows after index');
        let chunks = _.chunk(eventIds, 34464);
        log.debug('Chunked ' + eventIds.length + ' rows into ' + chunks.length + ' chunks.');
        for (const [index, chunk] of chunks.entries()) {
            const params = Array(chunk.length).fill(0).map((v, i) => `$${i+1}`)
            try {
                log.debug('Attempting to delete chunk ' + (index + 1) + ' of ' + chunks.length + ' chunks')
                const { rowCount } = await pgClient.query(
                    'DELETE FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) +
                    ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' IN (' + params.join(',') + ')',
                    chunk);
                log.debug('Attempted to delete ' + chunk.length + ' rows, actually deleted ' + rowCount + ' rows');
                return
            } catch(err) {
                log.error('Error when deleting rows from database', err);
                throw err
            }
        }
    }
});

const exit = async () => {
    log.log('Received SIGTERM, shutting down');
    log.log('Flushing remaining queue');
    esClient.clearInterval();
    try {
        await esClient.flush()
        log.debug('Flushed indexQueue');

        historic.stop()
        log.debug('Stopped Historic')

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

pgClient.init()
    .then(() => {
        historic.run()
            .then(() => {
                pgClient.start().then(() => {
                    esClient.begin()
                })
            })
            .catch(err => {
                log.fatal("Error processing historic")
                process.exit(1)
            });
    })