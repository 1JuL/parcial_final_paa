// test-mongo.js
require('dotenv').config();

const {MongoClient} = require("mongodb");
const { setServers } = require("node:dns/promises")

setServers(["1.1.1.1", "8.8.8.8"]);



const conn = new MongoClient(process.env.MONGO_URL);

async function test() {
  try {
    const mongoClient = await conn.connect();
    console.log("Mongo chambeando correctamente");
    mongoClient.close();
    process.exit(0);    

  } catch (err) {
    console.error('Error:', err.message);
  }
}

test();