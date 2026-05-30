const express = require('express');
require("dotenv").config();
const { createTransfer } = require('./command/transfer.command');
const { getTransfers }   = require('./query/transfer.query');
const { connectProducer } = require('./kafka/producer');
const { startConsumer }   = require('./kafka/consumer');

const API_PORT = process.env.API_PORT || 8080; 
const app = express();
app.use(express.json());

// COMMAND
app.post('/transfers', async (req, res) => {
  const { from, to, amount } = req.body;
  const transfer = await createTransfer(from, to, amount);
  res.status(201).json({
    message: 'Transferencia registrada',
    transfer
  });
});

// QUERY
app.get('/transfers/:account', async (req, res) => {
  const data = await getTransfers(req.params.account);
  res.json(data);
});

// health-check
app.get('/health', async (req, res) => {
  res.json({status: "ok"});
});

async function bootstrap() {
  await connectProducer();
  await startConsumer();      
  app.listen(API_PORT, () =>{ 
    console.log(`Listening on http://localhost:${API_PORT}`);
  });
}

bootstrap();