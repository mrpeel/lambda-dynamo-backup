'use strict';

let aws = require('aws-sdk');
// let stream = require('stream');
let ReadableStream = require('./readable-stream');
let zlib = require('zlib');
let async = require('async');
const lambda = new aws.Lambda({
  region: 'ap-southeast-2',
});

const moment = require('moment-timezone');

let ts = moment().tz('Australia/Sydney').format('YYYYMMDD');

aws.config.update({
  region: 'ap-southeast-2',
});

aws.config.loadFromPath('./credentials/aws.json');


let dynamo = new aws.DynamoDB();

let backupTable = function(tablename, lts, callback) {
  let md1 = moment();
  let dataStream = new ReadableStream(); // new stream.Readable();
  let gzip = zlib.createGzip();

  // create parameters hash for table scan
  let params = {
    TableName: tablename,
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
      Key: tablename + '/' + tablename + '-' + ts + '.gz',
    },
  });
  s3obj.upload({
    Body: body,
  }).on('httpUploadProgress', function(evt) {
    console.log(evt);
  }).send(function(err, data) {
    if (err) {
      console.log(err);
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
        console.log(`Backup ${tablename} took ${seconds} seconds.`);
      }
    }
  };

  // describe the table and write metadata to the backup
  dynamo.describeTable({
    TableName: tablename,
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

let backupAll = function(context, lts) {
  lts = lts || false;

  dynamo.listTables({}, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else {
      async.each(data.TableNames, function(table, callback) {
        console.log('Backing up ' + table);
        backupTable(table, lts, callback);
      }, function(err) {
        if (err) {
          console.log('A table failed to process');
        } else {
          console.log('All tables have been processed successfully');
        }
      // context.done(err);
      });
    }
  });
};

let invokeLambda = function(lambdaName) {
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

module.exports.backupAll = backupAll({}, false);
