let kafka = require('kafka-node')

let options = {
  kafkaHost: 'kb1.local.thsp.tech:19092,kb2.local.thsp.tech:29092,kb3.local.thsp.tech:39092',
  autoCommit: false,
  groupId: 'test-consumer-stream',
  sessionTimeout: 10000,
  protocol: ['roundrobin'],
  encoding: 'utf8'
};

consumer = new kafka.ConsumerGroupStream(options, 'TestTopic');

consumer.on('data', (message) => {
  console.info(message);
  commit(message);
});

consumer.on('error', (err) => {
  console.error(err, 'Kafka Error');
});

function commit(message) {
  consumer.commit(message, true);
}