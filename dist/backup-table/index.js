'use strict';

const aws = require('aws-sdk');
const ReadableStream = require('../libs/readable-stream');
const sns = require('../libs/publish-sns');
const zlib = require('zlib');
const asyncify = require('asyncawait/async');
const awaitify = require('asyncawait/await');
const moment = require('moment-timezone');
const snsArn = 'arn:aws:sns:ap-southeast-2:815588223950:lambda-activity';
const lambda = new aws.Lambda({
  region: 'ap-southeast-2',
});
const maxRecordsPerInvocation = 150000;

let ts = moment().tz('Australia/Sydney').format('YYYYMMDD');

aws.config.update({
  region: 'ap-southeast-2',
});

// aws.config.loadFromPath('../../credentials/aws.json');

let dynamo = new aws.DynamoDB();

let backupHandler = asyncify(function(event, context) {
  let md1 = moment();
  let dataStream = new ReadableStream();
  let gzip = zlib.createGzip();
  let backupCount = 0;
  let fileName = '';
  let invokeAgain = false;
  let eventDetails = {};

  let lts = event.lts || false;
  if (!event.table) {
    console.error('backupTable missing context.table');
    try {
      awaitify(
        sns.publishMsg(snsArn,
          'backupTable missing context.table',
          'Lambda backupTable error'));
    } catch (err) {}
    context.fail('backupTable missing context.table');
  }

  let tableName = event.table;


  // create parameters hash for table scan
  let params = {
    TableName: tableName,
    ReturnConsumedCapacity: 'NONE',
    Limit: '1',
  };

  // body will contain the compressed content to ship to s3
  let body = dataStream.pipe(gzip);
  let bucket;

  if (lts) {
    bucket = 'sharecast-lts-backup';
  } else {
    bucket = 'sharecast-backup';
  }

  fileName = tableName + '/' + tableName + '-' + ts + '.gz';

  let s3obj = new aws.S3({
    params: {
      Bucket: bucket,
      Key: fileName,
    },
  });

  s3obj.upload({
    Body: body,
  }, function(err) {
    if (err) {
      console.error(err);
      try {
        sns.publishMsg(snsArn,
          err,
          `Lambda backupTable ${tableName} failed`);
      } catch (err) {}
      context.fail(err);
    } else {
      finishedUpload(context, invokeAgain, eventDetails);
    }
  }).on('httpUploadProgress', function(evt) {
    console.log(evt);
  });

  let onScan = function(err, data) {
    if (err) {
      console.log(err, err.stack);
    } else {
      for (let idx = 0; idx < data.Items.length; idx++) {
        dataStream.append(JSON.stringify(data.Items[idx]));
        dataStream.append('\n');
        backupCount++;
      }

      if (typeof data.LastEvaluatedKey !== 'undefined' &&
        backupCount >= maxRecordsPerInvocation) {
        // Reached max records, need to reinvoke with params
        eventDetails.table = tableName;
        eventDetails.exclusiveStartKey = data.LastEvaluatedKey;
        eventDetails.partialUpdate = true;
        invokeAgain = true;

        let md2 = moment();
        let seconds = Math.abs(md1.diff(md2, 'seconds'));

        console.log(`Reached maximum records (${maxRecordsPerInvocation}) for`,
          ` backup.  Partial backup of ${tableName} took ${seconds} seconds.  `,
          `Reinvoking at: ${JSON.stringify(eventDetails.exclusiveStartKey)}`);

        dataStream.end();

        try {
          awaitify(
            sns.publishMsg(snsArn,
              `Reached maximum records for backup.  ` +
              `Partial backup of ${tableName} took ${seconds} seconds.  ` +
              `Reinvoking at: ` +
              `${JSON.stringify(eventDetails.exclusiveStartKey)}`,
              `Lambda backupTable ${tableName} partially completed`));
        } catch (err) {}
      // context.succeed();
      } else if (typeof data.LastEvaluatedKey !== 'undefined') {
        params.ExclusiveStartKey = data.LastEvaluatedKey;
        dynamo.scan(params, onScan);
      } else {
        dataStream.end();
        let md2 = moment();
        let seconds = Math.abs(md1.diff(md2, 'seconds'));
        console.log(`Backup ${tableName} took ${seconds} seconds.`);
        try {
          awaitify(
            sns.publishMsg(snsArn,
              `Backup ${tableName} took ${seconds} seconds.`,
              `Lambda backupTable ${tableName} succeeded`));
        } catch (err) {}

      }
    }
  };

  // describe the table and write metadata to the backup
  try {
    let table = awaitify(describeTable(tableName));

    if (event.partialUpdate) {
      let existingData = awaitify(retrievePartialBackup(bucket, fileName));
      dataStream.append(existingData);
    } else {
      // Starting a backup, so write table metadata to first line
      dataStream.append(JSON.stringify(table));
      dataStream.append('\n');
    }

    // limit the the number or reads to match our capacity
    params.Limit = table.ProvisionedThroughput.ReadCapacityUnits;

    if (event.exclusiveStartKey) {
      // Continuing a backup, so set-up key param
      params.ExclusiveStartKey = event.exclusiveStartKey;
    }

    // start streaming table data
    dynamo.scan(params, onScan);
  } catch (err) {
    console.error(err);
    try {
      awaitify(
        sns.publishMsg(snsArn,
          err,
          `Lambda backupTable ${tableName} failed`));
    } catch (err) {}
  }
});

let retrievePartialBackup = function(bucket, fileName) {
  return new Promise(function(resolve, reject) {
    let s3obj = new aws.S3();

    s3obj.getObject({
      Bucket: bucket,
      Key: fileName
    }, (err, data) => {
      // Handle any error and exit
      if (err) {
        reject(err);
      }

      zlib.gunzip(data.Body, (err, buffer) => {
        if (err) {
          reject(err);
        }
        resolve(buffer.toString());
      });
    });
  });
};

let finishedUpload = asyncify(function(context, invokeAgain, event) {
  // Check whether further processing is required to finish backup
  if (invokeAgain) {
    awaitify(invokeLambda('backupDynamo', event));
  /*awaitify(new Promise(function(resolve, reject) {
    backupHandler(event, context);
    resolve(true);
  }));*/
  }

  context.succeed();
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

let describeTable = function(tableName) {
  return new Promise(function(resolve, reject) {
    dynamo.describeTable({
      TableName: tableName,
    }, function(err, data) {
      if (err) {
        reject(err);
      } else {
        resolve(data.Table);
      }
    });
  });
};


module.exports.backupHandler = backupHandler;
