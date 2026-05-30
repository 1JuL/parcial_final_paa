import { Pool } from 'pg';
import { env } from './env';

export const pgPool = new Pool({
  connectionString: env.DATABASE_URL,
  ssl:              { rejectUnauthorized: false },  // Requerido por Supabase
  max:              10,
  idleTimeoutMillis:       30000,
  connectionTimeoutMillis: 5000,
});

pgPool.on('error', (err) => {
  console.error('[PostgreSQL] Error inesperado en el pool:', err);
});
