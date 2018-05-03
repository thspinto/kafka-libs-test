'use strict';
const kafka = require('kafka-node');

let producer;

function init() {
  const client = new kafka.KafkaClient(
    {
      kafkaHost: 'kb1.local.thsp.tech:19092,kb2.local.thsp.tech:29092,kb3.local.thsp.tech:39092',
      connectRetryOptions: {
        retries: 3,
        factor: 0,
        minTimeout: 1000,
        maxTimeout: 1000,
        randomize: true
      }
    });
  producer = new kafka.HighLevelProducer(client, { requireAcks: 1 } );
  const timeout = 10000

  const successPromise = new Promise((resolve) => {
    producer.on('ready', function () {
      console.info('Producer is ready');
      resolve();
    });
  });

  const timeOutPromise = new Promise((resolve, reject) => {
    setTimeout(() => {
      reject(new Error('Failed to connect to kafka after ' + timeout + ' ms.'));
    }, timeout);
  });

  return Promise.race([
    successPromise,
    timeOutPromise
  ]);
}

let i = 0
function send(message, topic) {
  console.info(`Producing message, to topic ${topic}`);
  message += i;
  i += 1;

  let payloads = [{
    topic: topic,
    messages: [message]
  }];

  producer.send(payloads, function (error, res) {
    if (error) {
      console.error('Failed to write message to Kafka: ' + message, error);
    } else {
      console.info('message written back to Kafka ' + JSON.stringify(res));
    }
  });
}

init().then(() => {
  setInterval(send, 10000, 'funky', 'TestTopic');
});