'use strict';

const aws = require('aws-sdk');
const ReadableStream = require('../libs/readable-stream');
const sns = require('../libs/publish-sns');
const zlib = require('zlib');
const asyncify = require('asyncawait/async');
const awaitify = require('asyncawait/await');


const moment = require('moment-timezone');

const snsArn = 'arn:aws:sns:ap-southeast-2:815588223950:lambda-activity';

let ts = moment().tz('Australia/Sydney').format('YYYYMMDD');

aws.config.update({
  region: 'ap-southeast-2',
});

let dynamo = new aws.DynamoDB();

let backupHandler = asyncify(function(event, context) {
  let md1 = moment();
  let dataStream = new ReadableStream();
  let gzip = zlib.createGzip();

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

  let s3obj = new aws.S3({
    params: {
      Bucket: bucket,
      Key: tableName + '/' + tableName + '-' + ts + '.gz',
    },
  });
  s3obj.upload({
    Body: body,
  }).on('httpUploadProgress', function(evt) {
    console.log(evt);
  }).send(function(err, data) {
    if (err) {
      console.error(err);
      try {
        awaitify(
          sns.publishMsg(snsArn,
            err,
            `Lambda backupTable ${tableName} failed`));
      } catch (err) {}
      context.fail(err);
    } else {
      context.succeed();
    }
  });

  let onScan = function(err, data) {
    if (err) console.log(err, err.stack);
    else {
      for (let idx = 0; idx < data.Items.length; idx++) {
        dataStream.append(JSON.stringify(data.Items[idx]));
        dataStream.append('\n');
      }

      if (typeof data.LastEvaluatedKey !== 'undefined') {
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
    // Write table metadata to first line
    dataStream.append(JSON.stringify(table));
    dataStream.append('\n');

    // limit the the number or reads to match our capacity
    params.Limit = table.ProvisionedThroughput.ReadCapacityUnits;

    // start streaminf table data
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
