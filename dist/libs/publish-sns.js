'use strict';

const aws = require('aws-sdk');

aws.config.update({
  region: 'ap-southeast-2',
});

const sns = new aws.SNS();

let publishMsg = function(topicArn, message, subject) {
  return new Promise(function(resolve, reject) {

    let params = {
      TopicArn: topicArn,
      Message: message,
    };
    if (subject) {
      params.Subject = subject;
    }

    sns.publish(params, function(err, data) {
      if (err) {
        // console.log('Error sending a message ', err);
        reject(err);
      } else {
        // console.log('Sent message: ', data.MessageId);
        resolve(data.MessageId);

      }
    });
  });
};

module.exports = {
  publishMsg: publishMsg
};
