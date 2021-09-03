const moment = require("moment");
const config = require("./config");

const timestamp = () => config.LOG_TIMESTAMP ? `[${moment().format('YYYY-MM-DD HH:mm:ss')}]` : ''

module.exports = {
    log: function(...args) {
        console.log(timestamp(), ...args);
    },
    debug: function(...args) {
        if (config.DEBUG) {
            console.debug(timestamp(), ...args);
        }
    },
    info: function(...args) {
        if (config.INFO) {
            console.debug(timestamp(), ...args);
        }
    },
    fatal: function(...args) {
        console.error(timestamp(), ...args);
        process.exit();
    },
    error: function(...args) {
        console.error(timestamp(), ...args);
    }
};


