'use strict';

const aws = require('aws-sdk');

aws.config.loadFromPath('./credentials/aws.json');

const dynamo = new aws.DynamoDB();
const lambda = new aws.Lambda({
  region: 'ap-southeast-2',
});

let invokeLambda = function(lambdaName, event) {
  lambda.invoke({
    FunctionName: lambdaName,
    InvocationType: 'Event',
    Payload: JSON.stringify(event, null, 2),
  }, function(err, data) {
    if (err) {
      console.log(err);
    } else {
      console.log(`Function ${lambdaName} executed with event: `,
        `${JSON.stringify(event)}`);
    }
  });
};

let backupAll = function() {
  dynamo.listTables({}, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      data.TableNames.forEach((table) => {
        console.log('Backing up ' + table);
        invokeLambda('backupDynamo', {
          table: table,
        });
      });
      console.log('All table backups have been invoked successfully');
    }
  });
};


backupAll();
