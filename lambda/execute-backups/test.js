'use strict';

const executeBackups = require('./index');

let context = {
  succeed: function(msg) {
    console.log('context.succeed(', msg || '', ')');
  },
  fail: function(msg) {
    console.log('context.fail(', msg || '', ')');
  },
}

backupTable.executeBackupsHandler({}, context);
