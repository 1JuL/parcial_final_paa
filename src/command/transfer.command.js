const { Pool } = require('pg');
const { publishTransfer } = require('../kafka/producer');

const pg = new Pool({ connectionString: process.env.PG_URL });

async function createTransfer(from, to, amount) {
  // 1. Escritura ACID en PostgreSQL
  const { rows } = await pg.query(
    `INSERT INTO transfers (from_account, to_account, amount)
     VALUES ($1, $2, $3) RETURNING *`,
    [from, to, amount]
  );

  const transfer = rows[0];

  // 2. Publicar evento en Kafka (no bloquea la respuesta)
  await publishTransfer(transfer);

  return transfer;
}

module.exports = { createTransfer };