const { MongoClient } = require('mongodb');
const { setServers } = require("node:dns/promises")

setServers(["1.1.1.1", "8.8.8.8"]);
const mongo = new MongoClient(process.env.MONGO_URL);

async function getTransfers(account) {
  await mongo.connect();
  return mongo.db('banco')
    .collection('transfers_read')
    .find({ $or: [{ fromAccount: account }, { toAccount: account }] })
    .sort({ createdAt: -1 })
    .toArray();
}

module.exports = { getTransfers };