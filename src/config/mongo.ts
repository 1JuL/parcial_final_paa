import mongoose from 'mongoose';
import { env } from './env';
import { setServers } from 'node:dns/promises';

export async function connectMongo(): Promise<void> {
  // Configurar servidores DNS públicos para evitar ECONNREFUSED en la resolución SRV de Atlas
  setServers(['1.1.1.1', '8.8.8.8']);
  
  await mongoose.connect(env.MONGO_URI, { dbName: 'banco_dhabi' });
  console.log('[MongoDB] Conectado a Atlas');
}
