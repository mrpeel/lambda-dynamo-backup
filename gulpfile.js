'use strict';

const gulp = require('gulp');
const install = require('gulp-install');
const zip = require('gulp-zip');
const del = require('del');
const awsLambda = require('gulp-aws-lambda');
const credentials = require('./credentials/aws.json');

const lambdaParams1 = {
  FunctionName: 'backupDynamo',
  Handler: 'backup-table/index.backupHandler',
  Role: 'arn:aws:iam::815588223950:role/lambda_backp_dynamo_to_s3',
  Runtime: 'nodejs6.10',
  Description: 'Backup a single dynamodb table',
  MemorySize: 512,
  Timeout: 300,
  Publish: true,
  Code: {
    S3Bucket: 'cake-lambda-zips',
    S3Key: 'backup-dynamo.zip',
  },
};

const lambdaParams2 = {
  FunctionName: 'executeAllDynamoBackups',
  Handler: 'execute-backups/index.executeBackupsHandler',
  Role: 'arn:aws:iam::815588223950:role/lambda_backp_dynamo_to_s3',
  Runtime: 'nodejs6.10',
  Description: 'Trigger all dynamo table backups',
  MemorySize: 128,
  Timeout: 300,
  Publish: true,
  Code: {
    S3Bucket: 'cake-lambda-zips',
    S3Key: 'trigger-backup-dynamo.zip',
  },
};

const lambdaParams3 = {
  FunctionName: 'exportCsv',
  Handler: 'export-csv/index.exportToCsvHandler',
  Role: 'arn:aws:iam::815588223950:role/lambda_backp_dynamo_to_s3',
  Runtime: 'nodejs6.10',
  Description: 'Execute a csv export increment',
  MemorySize: 1024,
  Timeout: 300,
  Publish: true,
  Code: {
    S3Bucket: 'cake-lambda-zips',
    S3Key: 'export-csv.zip',
  },
};

const lambdaParams4 = {
  FunctionName: 'executeExportCsv',
  Handler: 'execute-export/index.executeExportHandler',
  Role: 'arn:aws:iam::815588223950:role/lambda_backp_dynamo_to_s3',
  Runtime: 'nodejs6.10',
  Description: 'Execute a csv export',
  MemorySize: 1024,
  Timeout: 300,
  Publish: true,
  Code: {
    S3Bucket: 'cake-lambda-zips',
    S3Key: 'trigger-export-csv.zip',
  },
};


const awsCredentials = {
  accessKeyId: credentials['accessKeyId'],
  secretAccessKey: credentials['secretAccessKey'],
  region: credentials['region'],
};

gulp.task('clean', function() {
  return del(['./dist/**/*']);
});

gulp.task('js', ['clean'], function() {
  return gulp.src(['./lambda/**/*'])
    .pipe(gulp.dest('dist/'));
});


gulp.task('install_dependencies', ['js'], function() {
  return gulp.src('./package.json')
    .pipe(gulp.dest('./dist'))
    .pipe(install({
      production: true,
    }));
});

gulp.task('deploy', ['install_dependencies'], function() {
  gulp.src(['dist/**/*'])
    .pipe(zip('backup-dynamo.zip'))
    .pipe(awsLambda(awsCredentials, lambdaParams1));

  gulp.src(['dist/**/*'])
    .pipe(zip('trigger-backup-dynamo.zip'))
    .pipe(awsLambda(awsCredentials, lambdaParams2));
});

gulp.task('deployCsv', ['install_dependencies'], function() {
  gulp.src(['dist/**/*'])
    .pipe(zip('export-csv.zip'))
    .pipe(awsLambda(awsCredentials, lambdaParams3));

  gulp.src(['dist/**/*'])
    .pipe(zip('trigger-export-csv.zip'))
    .pipe(awsLambda(awsCredentials, lambdaParams4));
});
