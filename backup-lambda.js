'use strict';

let aws = require('aws-sdk');
let ReadableStream = require('./readable-stream');
let zlib = require('zlib');
let async = require('async');

const moment = require('moment-timezone');

let ts = moment().tz('Australia/Sydney').format('YYYYMMDD');

aws.config.update({
  region: 'ap-southeast-2',
});

let dynamo = new aws.DynamoDB();

let backupHandler = function(event, context) {
  let md1 = moment();
  let dataStream = new ReadableStream();
  let gzip = zlib.createGzip();

  let lts = event.lts || false;
  if (!event.table) {
    console.error('backupTable missing context.table');
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
      }
    }
  };

  // describe the table and write metadata to the backup
  dynamo.describeTable({
    TableName: tableName,
  }, function(err, data) {
    if (err) console.log(err, err.stack);
    else {
      let table = data.Table;
      // Write table metadata to first line
      dataStream.append(JSON.stringify(table));
      dataStream.append('\n');

      // limit the the number or reads to match our capacity
      params.Limit = table.ProvisionedThroughput.ReadCapacityUnits;

      // start streaminf table data
      dynamo.scan(params, onScan);
    }
  });
};

module.exports.backupHandler = backupHandler;
