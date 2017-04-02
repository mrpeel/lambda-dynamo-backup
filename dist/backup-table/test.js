'use strict';

const backupTable = require('./index');

let context = {
  succeed: function(msg) {
    console.log('context.succeed(', msg || '', ')');
  },
  fail: function(msg) {
    console.log('context.fail(', msg || '', ')');
  },
};

backupTable.backupHandler({
  table: 'companyQuotes',
  fileIncrement: 1,
  partialUpdate: true,
}, context);
