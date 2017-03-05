const aws = require('aws-sdk');

aws.config.loadFromPath('./credentials/aws.json');

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

let testPublish = function() {
  publishMsg('arn:aws:sns:ap-southeast-2:815588223950:lambda-activity',
    'This is a messgae.  There are many others like it but this one if mine.',
    'First node js message')
    .then((msgId) => {
      console.log(`Success, message Id: ${msgId}`);
    })
    .catch((err) => {
      console.error(err);
    });
}

testPublish();

module.exports = {
  publishMsg: publishMsg
};
