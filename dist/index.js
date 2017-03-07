'use strict';

const tableBackup = require('./backup-lambda');
const executeBackups = require('./execute-backups-lambda');

exports = {
  backupTableHandler: function(event, context) {
    tableBackup.backupHandler(event, context);
  },
  executeBackupsHandler: function(event, context) {
    executeBackups.executeBackupsHandler(event, context);
  },
};
