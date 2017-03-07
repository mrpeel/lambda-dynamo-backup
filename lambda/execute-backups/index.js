'use strict';

const aws = require('aws-sdk');
const sns = require('../libs/publish-sns');
const asyncify = require('asyncawait/async');
const awaitify = require('asyncawait/await');

const snsArn = 'arn:aws:sns:ap-southeast-2:815588223950:lambda-activity';


const dynamo = new aws.DynamoDB();
const lambda = new aws.Lambda({
  region: 'ap-southeast-2',
});

let invokeLambda = function(lambdaName, event) {
  return new Promise(function(resolve, reject) {
    lambda.invoke({
      FunctionName: lambdaName,
      InvocationType: 'Event',
      Payload: JSON.stringify(event, null, 2),
    }, function(err, data) {
      if (err) {
        reject(err);
      } else {
        console.log(`Function ${lambdaName} executed with event: `,
          `${JSON.stringify(event)}`);
        resolve(true);
      }
    });
  });
};

let listTables = function(db) {
  return new Promise(function(resolve, reject) {
    db.listTables({}, function(err, data) {
      if (err) {
        reject(err);
      } else {
        resolve(data.TableNames);
      }
    });
  });
};

let executeBackupsHandler = asyncify(function(event, context) {
  try {
    let tableNames = awaitify(listTables(dynamo));

    tableNames.forEach((table) => {
      console.log('Backing up ' + table);
      awaitify(invokeLambda('backupDynamo', {
        table: table,
      }));
    });
    console.log('All table backups have been invoked successfully');
    context.succeed();
  } catch (err) {
    console.error(err, err.stack);
    try {
      awaitify(sns.publishMsg(snsArn,
        err,
        'Lambda executeBackupsHandler failed'));
    } catch (err) {}
    context.fail('executeBackupsHandler failed.  ', err);
  }
});

module.exports.executeBackupsHandler = executeBackupsHandler;
