require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  try {
    const file1 = fs.readFileSync(path.join(__dirname, 'migrations', '001_create_notifications.sql'), 'utf8');
    const file2 = fs.readFileSync(path.join(__dirname, 'migrations', '002_create_audit_log.sql'), 'utf8');

    console.log('Running migration 001...');
    await pool.query(file1);
    
    console.log('Running migration 002...');
    await pool.query(file2);

    console.log('Migrations completed successfully!');
  } catch (err) {
    console.error('Migration failed:', err);
  } finally {
    await pool.end();
  }
}

run();
