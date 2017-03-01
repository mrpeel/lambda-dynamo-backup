'use strict';

let aws = require('aws-sdk');
const lambda = new aws.Lambda({
  region: 'ap-southeast-2',
});

aws.config.loadFromPath('../credentials/aws.json');

let invokeLambda = function(lambdaName, event) {
  lambda.invoke({
    FunctionName: lambdaName,
    InvocationType: 'Event',
    Payload: JSON.stringify(event, null, 2),
  }, function(err, data) {
    if (err) {
      console.log(err); // an error occurred
    } else {
      console.log(`Function ${lambdaName} executed.`);
    }
  });
};

let backupAll = function() {
  lts = lts || false;

  dynamo.listTables({}, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      async.each(data.TableNames, function(table, callback) {
        console.log('Backing up ' + table);
        invokeLambda('backupDynamo', {
          table: table,
        });
      }, function(err) {
        if (err) {
          console.log('A table failed to process');
        } else {
          console.log('All tables have been processed successfully');
        }
      });
    }
  });
};


backupAll();
