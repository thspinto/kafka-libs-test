const sleep = require('sleep-promise');
const kafkaConsumerManager = require('kafka-consumer-manager');

async function handleMessageAsync(msg) {
  if (msg.value === 'e') {
    await sleep(10000);
  }
  console.info(msg);
}

function handleMessage(msg) {
  return new Promise((resolve) => {
    handleMessageAsync(msg).then(() => { resolve(); });
  });
}

const configuration = {
  KafkaUrl: 'localhost:9092',
  GroupId: 'test-consumer2',
  KafkaConnectionTimeout: 10000,
  KafkaOffsetDiffThreshold: 1,
  Topics: ['TestTopic'],
  MessageFunction: handleMessage,
  ThrottlingThreshold: 2,
  ThrottlingCheckIntervalMs: 500,
  AutoCommit: false,
};

kafkaConsumerManager.init(configuration).then(() => { console.info('consumer started'); });
