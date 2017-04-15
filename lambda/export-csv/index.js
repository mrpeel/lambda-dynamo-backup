'use strict';

const aws = require('aws-sdk');
const csv = require('fast-csv');
const fs = require('fs');
const zlib = require('zlib');
const s3StreamUpload = require('s3-stream-upload');
const asyncify = require('asyncawait/async');
const awaitify = require('asyncawait/await');
const moment = require('moment-timezone');

let ts = moment().tz('Australia/Sydney').format('YYYYMMDD');

aws.config.update({
  region: 'ap-southeast-2',
});


let dynamodb = new aws.DynamoDB({
  maxRetries: 20,
});

let s3 = new aws.S3();
// Total row count
let rowCount = 0;

// Writes out an item without regard for headers
let writeItemWithoutHeaders = function(stream, item) {
  let row = {};
  Object.keys(item).forEach((key) => {
    row[key] = item[key].S || item[key].N || item[key].BOOL;
  });

  return stream.write(row);
};

// Writes out an item and ensures that every specified column
// is represented
let writeItemWithHeaders = function(stream, item, columns) {
  let row = {};
  columns.forEach((column) => {
    if (item[column]) {
      row[column] = item[column].S || item[column].N || item[column].BOOL;
    } else {
      row[column] = '';
    }
  });

  return stream.write(row);
};

// Does the real work of writing the table to a CSV file
let writeTableToCsv = asyncify(function(options, context) {
  if (!options.table) {
    console.error('options.table not specified');
    return;
  }

  let params = {
    TableName: options.table,
  };

  if (options.filterExpression) {
    params.FilterExpression = options.filterExpression;
  }

  if (!options.filesize) {
    options.filesize = 250;
  }


  let csvStream;
  let backoff = 1;
  // Count of files used to increment number in filename for each file
  let fileCount = 0;
  let fileName;
  let fileRowCount = 0;
  let writableStream;


  let setupFileStream = function() {
    // Form the filename with the table name as the subdirectory and the base of the filename
    // then th segemnt and the file within the segment
    fileName = options.table + '-' + ts + '-' +
      ('000' + fileCount).slice(-3)
      + '.csv';

    if (options.compressed) {
      fileName += '.gz';
    }

    csvStream = csv.createWriteStream({
      headers: true,
      maxBufferSize: 10000,
    });

    if (options.s3Bucket) {
      let filePath = '';
      if (options.s3Path) {
        filePath += options.s3Path + '/';
      }
      filePath += options.table + '/' + options.fileName;
      writableStream = s3StreamUpload(s3, {
        Bucket: options.s3Bucket,
        Key: filePath,
      });
      console.log('Starting new file: s3://' + options.s3Bucket + '/' + filePath);
    } else {
      writableStream = fs.createWriteStream(options.table + '/' + fileName);
      console.log('Starting new file: ' + fileName);
    }

    // If we are compressing pipe it through gzip
    if (options.compressed) {
      csvStream.pipe(zlib.createGzip()).pipe(writableStream);
    } else {
      csvStream.pipe(writableStream);
    }

    fileRowCount = 0;
  };

  // Repeatedly scan dynamodb until there are no more rows
  let onScan = function(err, data) {
    let noDrainRequired = false;
    if (err) {
      // Check for throughput exceeded
      if (err.code && err.code == 'ProvisionedThroughputExceededException') {
        console.warn('ProvisionedThroughputExceededException, backing off');
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
      // Reset backoff
      backoff = 1;

      data.Items.forEach((item) => {
        if (fileRowCount === 0) {
          noDrainRequired = writeItemWithHeaders(csvStream, item, options.columns);
        } else {
          noDrainRequired = writeItemWithoutHeaders(csvStream, item);
        }

        fileRowCount++;
        rowCount++;

        console.log('Row: ' + rowCount + ', Mb: ' + (writableStream.bytesWritten / 1024 / 1024).toFixed(2));

        // Keep going if there is more data and we haven't exceeded the file size
        if (writableStream.bytesWritten >= 1024 * 1024 * options.filesize) {
          // End current file and set-up a new file
          csvStream.end();
          fileCount++;
          setupFileStream();
          dynamodb.scan(params, onScan);
        } else if (!noDrainRequired) {
          // Check if we need to drain to avoid bloating memory
          csvStream.once('drain', function() {
            dynamodb.scan(params, onScan);
          });
        } else {
          dynamodb.scan(params, onScan);
        }
      });
    } else {
      csvStream.end();
    }
  };

  // Wait for the stream to emit finish before we return
  // When gzipped this can take a bit
  writableStream.on('finish', function() {
    console.log('Finished file: ' + fileName);
  });
});

let sleep = function(ms) {
  if (!ms) {
    ms = 1;
  }
  return new Promise((r) => {
    setTimeout(r, ms);
  });
};


module.exports = writeTableToCsv;