const fs = require('fs');
const path = require('path')
const dotenv = require('dotenv');

const envPath = fs.existsSync(path.resolve(__dirname + '/.env')) ? path.resolve(__dirname + '/.env') : null;
envPath && dotenv.config(envPath);

let config = {
    DEBUG: parseInt(process.env.DEBUG) || 0 ? true : false, // Show debug + info messages
    INFO: ((parseInt(process.env.DEBUG) || 0) ? 1 : (parseInt(process.env.INFO) || 1)) ? true : false, // Show info messages
    PG_HOST: process.env.PG_HOST || 'localhost', // Host of the PostgreSQL database server
    PG_DATABASE: process.env.PG_DATABASE || 'databasename', // Name of the PostgreSQL database
    PG_USERNAME: process.env.PG_USERNAME || 'root', // Username of the PostgreSQL database
    PG_PASSWORD: process.env.PG_PASSWORD || '', // Password of the PostgreSQL database
    PG_LISTEN_TO: process.env.PG_LISTEN_TO || 'audit', // The LISTEN queue to listen on for the PostgreSQL database - https://www.postgresql.org/docs/9.1/static/sql-notify.html
    PG_LISTEN_TO_ID: process.env.PG_LISTEN_TO_ID || ((process.env.PG_LISTEN_TO || 'audit') + '_id'), // The LISTEN queue to listen on for the PostgreSQL database for big sets of data (data over 8000 characters cannot be sent via NOTIFY)
    PG_PORT: process.env.PG_PORT || 5432, // The port that the PostgreSQL database is listening on
    PG_TIMESTAMP_COLUMN: process.env.PG_TIMESTAMP_COLUMN || 'action_timestamp', // The column of the row that is used as the timestamp for Elasticsearch
    PG_UID_COLUMN: process.env.PG_UID_COLUMN || 'event_id', // The primary key column of the row that is stored in Elasticsearch
    PG_ORDER_BY_COLUMN: process.env.PG_ORDER_BY_COLUMN || 'action_timestamp', // The column of the row that is used to order the rows in PostgreSQL
    PG_DELETE_ON_INDEX: process.env.hasOwnProperty('PG_DELETE_ON_INDEX') ? parseInt(process.env.PG_DELETE_ON_INDEX) : 0, // Delete the rows in PostgreSQL after they have been indexed to Elasticsearch
    PG_SCHEMA: process.env.PG_SCHEMA || 'audit', // The schema of the table which rows which will be deleted from PG_DELETE_ON_INDEX
    PG_TABLE: process.env.PG_TABLE || 'logged_actions', // The table name which rows will be deleted from by PG_DELETE_ON_INDEX
    ES_LABEL_NAME: process.env.ES_LABEL_NAME || null,
    ES_LABEL: process.env.ES_LABEL || null,
    ES_HOST: process.env.ES_HOST || 'localhost', // The hostname for the Elasticsearch server (pooling not supported currently)
    ES_PORT: process.env.ES_PORT || 9200,  // The port for the Elasticsearch server
    ES_USERNAME: process.env.ES_USERNAME || null,  // The username for the Elasticsearch server
    ES_PASSWORD: process.env.ES_PASSWORD || null,  // The password for the Elasticsearch server
    ES_PROTO: process.env.ES_PROTO || 'https', // The protocol used for the Elasticsearch server connections
    ES_INDEX_PREFIX: process.env.ES_INDEX_PREFIX || 'audit', // The Elasticsearch index the data should be stored in
    ES_INDEX_APPEND_TABLE_NAME: process.env.ES_INDEX_APPEND_TABLE_NAME ? JSON.parse(process.env.ES_INDEX_APPEND_TABLE_NAME) : false, // Append the table name to the index
    ES_INDEX_DATE_SUFFIX_FORMAT: process.env.ES_INDEX_DATE_SUFFIX_FORMAT || null, // moment date format to create index suffix from
    ES_TYPE: process.env.ES_TYPE || '_doc', // The type of the data to be stored in Elasticsearch
    ES_MAPPING: process.env.ES_MAPPING ? JSON.parse(process.env.ES_MAPPING) : null,
    ES_ALLOW_INSECURE_SSL: process.env.ES_ALLOW_INSECURE_SSL ? JSON.parse(process.env.ES_ALLOW_INSECURE_SSL) : false,
    ES_PRE_CREATE_INDICIES: process.env.ES_PRE_CREATE_INDICIES ? JSON.parse(process.env.ES_PRE_CREATE_INDICIES) : false,
    ES_BULK_ACTION: process.env.ES_BULK_ACTION || 'create',
    QUEUE_LIMIT: process.env.QUEUE_LIMIT || 500, // The maximum number of items that should be queued before pushing to Elasticsearch
    QUEUE_TIMEOUT: process.env.QUEUE_TIMEOUT || 120, // The maximum seconds for an item to be in the queue before it is pushed to Elasticsearch
    LOG_TIMESTAMP: parseInt(process.env.LOG_TIMESTAMP) || 0
};

module.exports = config;