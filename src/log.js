const config = require("./config");

module.exports = {
    log: function(...args) {
        console.log(...args);
    },
    debug: function(...args) {
        if (config.DEBUG) {
            console.debug(...args);
        }
    },
    info: function(...args) {
        if (config.INFO) {
            console.debug(...args);
        }
    },
    fatal: function(...args) {
        console.error(...args);
        process.exit();
    },
    error: function(...args) {
        console.error(...args);
    }
};


