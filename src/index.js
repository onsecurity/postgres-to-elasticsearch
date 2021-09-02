const _ = require('lodash'),
    config = require('./config'),
    esClient = require('./esClient'),
    pgClient = require('./pgClient'),
    historic = require('./historic'),
    PgEscape = require('pg-escape'),
    log = require('./log');


esClient.onFlush(async function(indexQueue, dataQueue) {
    return new Promise(async (accept, reject) => {
        if (config.PG_DELETE_ON_INDEX) {
            let pg = await pgClient.client();
            let eventIds = _.map(dataQueue, index => index[config.PG_UID_COLUMN]);
            log.debug('UIDs to be deleted: ', eventIds);
            log.debug('Deleting ' + eventIds.length + ' rows after index');
            let chunks = _.chunk(eventIds, 34464);
            log.debug('Chunked ' + eventIds.length + ' rows into ' + chunks.length + ' chunks.');
            chunks.forEach((chunk, index) => {

                let params = [];
                for (let paramId = 1; paramId <= chunk.length; paramId++) {
                    params.push('$' + paramId);
                }

                pg.query(
                    'DELETE FROM ' + PgEscape.ident(config.PG_SCHEMA) + '.' + PgEscape.ident(config.PG_TABLE) +
                    ' WHERE ' + PgEscape.ident(config.PG_UID_COLUMN) + ' IN (' + params.join(',') + ')',
                    chunk,
                    (err, res) => {
                        if (err) {
                            log.error('Error when deleting rows from database', err);
                            reject();
                            return;
                        }
                        log.info('Attempted to delete ' + chunk.length + ' rows, actually deleted ' + res.rowCount + ' rows');
                        if (index === chunks.length - 1) {
                            accept();
                        }
                    }
                );
            });
        } else {
            return accept();
        }
    });
});

process.on('SIGTERM', function () {
    log.log('Received SIGTERM, shutting down');
    log.log('Flushing remaining queue');
    esClient.flush()
        .then(() => {
            log.debug('Flushed indexQueue');
            log.debug('Closing PG connection');
            pgClient.end()
                .then(() => {
                    log.debug('Closed PG connection');
                    log.log('Exiting gracefully');
                    process.exit(0);
                }).catch(() => {
                    log.error('Unable to flush queue, unable to quit gracefully')
                });
        }).catch(() => {
            log.error('Unable to close PG connection, unable to quit gracefully')
        });
});

historic.run();
pgClient.start();