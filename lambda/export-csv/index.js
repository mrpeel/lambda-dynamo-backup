'use strict';

const aws = require('aws-sdk');
const csv = require('fast-csv');
const zlib = require('zlib');
const asyncify = require('asyncawait/async');
const awaitify = require('asyncawait/await');
const moment = require('moment-timezone');
const lambda = new aws.Lambda({
  region: 'ap-southeast-2',
});
const sns = require('../libs/publish-sns');
const snsArn = 'arn:aws:sns:ap-southeast-2:815588223950:lambda-activity';


let ts = moment().tz('Australia/Sydney').format('YYYYMMDD');

aws.config.update({
  region: 'ap-southeast-2',
});

const client = new aws.DynamoDB.DocumentClient({
  maxRetries: 100,
});

// Total row count
let rowCount = 0;

// Writes out an item without regard for headers
let writeItemWithoutHeaders = function(stream, item) {
  let row = {};
  Object.keys(item).forEach((key) => {
    // row[key] = item[key].S || item[key].N || item[key].BOOL;
    row[key] = item[key];
  });

  return stream.write(row);
};

// Writes out an item and ensures that every specified column
// is represented
let writeItemWithHeaders = function(stream, item, columns) {
  let row = {};
  columns.forEach((column) => {
    if (item[column]) {
      // row[column] = item[column].S || item[column].N || item[column].BOOL;
      row[column] = item[column];
    } else {
      row[column] = '';
    }
  });

  return stream.write(row);
};

// Does the real work of writing the table to a CSV file
let exportToCsvHandler = asyncify(function(event, context) {
  if (!event.table) {
    console.error('event.table not specified');
    return;
  }

  if (!event.s3Bucket) {
    console.error('event.s3Bucket must be specified');
    return;
  }

  let invokeAgain = false;
  let eventDetails = {};

  let tableName = event.table;
  let s3Bucket = event.s3Bucket;
  let filePath = '' || event.s3Path + '/';
  let md1 = moment();
  let fileIncrement = 1;
  let gzip = zlib.createGzip();

  // Set-up details for e-invoking the next increment
  eventDetails.s3Bucket = s3Bucket;
  eventDetails.s3Path = event.s3Path;
  eventDetails.columns = event.columns;

  if (event.fileIncrement) {
    fileIncrement = event.fileIncrement + 1;
  }

  let params = {
    TableName: tableName,
  };

  console.log('Starting csv export function ...');

  if (event.filterExpression && event.expressionAttributeValues) {
    params.FilterExpression = event.filterExpression;
    params.ExpressionAttributeValues = event.expressionAttributeValues;
    // Set-up details for re-invoking the next increment
    eventDetails.filterExpression = event.filterExpression;
    eventDetails.expressionAttributeValues = event.expressionAttributeValues;
    console.log(`Filter expression: ${params.FilterExpression}`);
    console.log(`Expression values: ${JSON.stringify(params.ExpressionAttributeValues)}`);
  }

  if (event.exclusiveStartKey) {
    params.ExclusiveStartKey = event.exclusiveStartKey;
    console.log(`Starting at: ${JSON.stringify(params.ExclusiveStartKey)}`);
  }


  let csvStream;
  let backoff = 1;
  // Count of files used to increment number in filename for each file
  let fileName;
  let fileRowCount = 0;

  let setupFileStream = function() {
    // Form the filename with the table name as the subdirectory and the base of the filename
    // then th segemnt and the file within the segment
    fileName = tableName + '-' + ts + '-' +
      ('000' + fileIncrement).slice(-3) + '.csv.gz';


    csvStream = csv.createWriteStream({
      headers: true,
      maxBufferSize: 10000,
    });

    // Stream to gzip
    let body = csvStream.pipe(gzip);

    filePath += fileName;

    console.log(`Creating: ${s3Bucket} / ${filePath}`);

    let s3obj = new aws.S3({
      params: {
        Bucket: s3Bucket,
        Key: filePath,
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
            `Lambda exportToCSV ${tableName} failed`);
        } catch (err) {
          console.log('Publish to SNS failed');
        }
        context.fail(err);
      } else {
        finishedUpload(context, invokeAgain, eventDetails);
      }
    }).on('httpUploadProgress', function(evt) {
      console.log(evt);
    });

    fileRowCount = 0;
  };

  let drainMemory = function(csvStreamToDrain) {
    return new Promise(function(resolve, reject) {
      csvStreamToDrain.once('drain', function() {
        resolve(true);
      });
    });
  };

  // Repeatedly scan dynamodb until there are no more rows
  let onScan = asyncify(function(err, data) {
    let drainRequired = false;
    let finishedScan = false;
    if (err) {
      // Check for throughput exceeded
      if (err.code && err.code == 'ProvisionedThroughputExceededException') {
        console.log('ProvisionedThroughputExceededException, backing off');
        // Wait at least one second before the next query
        awaitify(sleep(backoff * 1000));
        // Increment backoff
        backoff *= 2;
      } else {
        console.error(err);
        return;
      }
    } else if (typeof data.LastEvaluatedKey !== 'undefined') {
      params.ExclusiveStartKey = data.LastEvaluatedKey;
    } else {
      finishedScan = true;
    }
    // Reset backoff
    backoff = 1;

    data.Items.forEach((item) => {
      if (fileRowCount === 0) {
        drainRequired = !writeItemWithHeaders(csvStream, item, event.columns);
      } else {
        drainRequired = !writeItemWithoutHeaders(csvStream, item);
      }

      fileRowCount++;
      rowCount++;

      if (rowCount % 10000 === 0) {
        console.log('Row: ' + rowCount);
      }
    });

    let md2 = moment();
    let seconds = Math.abs(md1.diff(md2, 'seconds'));

    // Keep going if there is more data and we haven't exceeded the file size
    if (finishedScan) {
      // Last record so end csv stream
      console.log(`Export ${tableName} took ${seconds} seconds.`);
      try {
        awaitify(
          sns.publishMsg(snsArn,
            `Export ${tableName} took ${seconds} seconds.`,
            `Lambda exportToCsv ${tableName} succeeded`));
      } catch (err) {
        console.log('Publish to SNS failed');
      }

      csvStream.end();
    } else {
      // Reached max time for lambda so set-up to finish and re-invoke
      if (typeof data.LastEvaluatedKey !== 'undefined' &&
        seconds >= 250) {
        eventDetails.table = tableName;
        eventDetails.exclusiveStartKey = data.LastEvaluatedKey;
        eventDetails.partialUpdate = true;
        eventDetails.fileIncrement = fileIncrement;
        invokeAgain = true;

        console.log(`Reached maximum execution time for`,
          ` export.  Partial export of ${tableName} took ${seconds} seconds.  `,
          `Reinvoking at: ${JSON.stringify(eventDetails.exclusiveStartKey)}`);

        // End current file and set-up a new file
        csvStream.end();

        try {
          awaitify(
            sns.publishMsg(snsArn,
              `Reached maximum time for export.  ` +
              `Partial export of ${tableName} took ${seconds} seconds.  ` +
              `Reinvoking at: ` +
              `${JSON.stringify(eventDetails.exclusiveStartKey)}`,
              `Lambda exportCsv ${tableName} partially completed`));
        } catch (err) {
          console.log('Publish to SNS failed');
        }
      } else {
        // Drain to avoid bloating memory
        if (drainRequired) {
          awaitify(drainMemory(csvStream));
        }

        // Continue with scan
        client.scan(params, onScan);
      }
    }
  });

  // Set-up file and start first scan
  setupFileStream();
  client.scan(params, onScan);
});

let sleep = function(ms) {
  if (!ms) {
    ms = 1;
  }
  return new Promise((r) => {
    setTimeout(r, ms);
  });
};

let finishedUpload = asyncify(function(context, invokeAgain, event) {
  // Check whether further processing is required to finish backup
  if (invokeAgain) {
    awaitify(invokeLambda('exportCsv', event));
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

module.exports.exportToCsvHandler = exportToCsvHandler;
