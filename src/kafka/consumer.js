const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const { setServers } = require("node:dns/promises")
setServers(["1.1.1.1", "8.8.8.8"]);


const kafka = new Kafka({
  clientId: "CQRS-consumer",
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

const consumer = kafka.consumer({ groupId: 'mongo-sync-group' });
const mongo = new MongoClient(process.env.MONGO_URL);

async function startConsumer() {
  // await mongo.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'transfers', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const transfer = JSON.parse(message.value.toString());

      await mongo.db('banco')
        .collection('transfers_read')
        .insertOne({
          transfer_id: transfer.id,
          fromAccount: transfer.from_account,
          toAccount:   transfer.to_account,
          amount:      transfer.amount,
          createdAt:   transfer.created_at,
        });

      console.log(`Evento recibido y replicado -> transfer ${transfer.id}`);
    }
  });
}

module.exports = { startConsumer };