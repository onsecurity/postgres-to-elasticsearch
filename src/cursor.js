const { promisify } = require('util')
const Cursor = require('pg-cursor')

Cursor.prototype.readAsync = promisify(Cursor.prototype.read)

module.exports = Cursor;