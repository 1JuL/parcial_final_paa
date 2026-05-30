const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: "CQRS-producer",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,

  sasl: {
    mechanism: 'plain',
    username: process.env.API_KEY,
    password: process.env.API_SECRET,
  },
  retry: {
    initialRetryTime: 300,
    retries: 10,
  },
});

const producer = kafka.producer();

async function connectProducer() {
  await producer.connect();
  console.log('Kafka producer conectado');
}

async function publishTransfer(transfer) {
  await producer.send({
    topic: 'transfers',
    messages: [{
      key: String(transfer.id),
      value: JSON.stringify(transfer),  // el evento completo
    }]
  });
}

module.exports = { connectProducer, publishTransfer };