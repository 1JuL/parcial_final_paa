import 'dotenv/config';
import express                    from 'express';
import path                       from 'path';
import { pgPool }                 from './config/database';
import { connectMongo }           from './config/mongo';
import { setupKafkaTopics, kafkaProducer } from './config/kafka';
import { notificationRouter }     from './api/notification.controller';
import { demoRouter }             from './api/demo.controller';
import { startMongoSyncConsumer } from './kafka/consumers/mongo-sync.consumer';
import { startAuditConsumer }     from './kafka/consumers/audit.consumer';
import { startRetryConsumer }     from './kafka/consumers/retry.consumer';
import { env }                    from './config/env';

async function bootstrap(): Promise<void> {
  console.log('[Bootstrap] Iniciando Banco Dhabi SMS Service v2.0...');

  // 1. Conectar PostgreSQL
  await pgPool.connect();
  console.log('[Bootstrap] PostgreSQL conectado (Supabase)');

  // 2. Conectar MongoDB
  await connectMongo();
  console.log('[Bootstrap] MongoDB conectado (Atlas)');

  // 3. Crear topics Kafka y conectar producer
  await setupKafkaTopics();
  await kafkaProducer.connect();
  console.log('[Bootstrap] Kafka producer conectado');

  // 4. Iniciar consumers Kafka (en paralelo)
  // El orden de bootstrap es obligatorio: consumers DESPUÉS del producer
  await Promise.all([
    startMongoSyncConsumer(),
    startAuditConsumer(),
    startRetryConsumer(),
  ]);
  console.log('[Bootstrap] Todos los consumers Kafka iniciados');

  // 5. Iniciar servidor Express
  const app = express();
  app.use(express.json());

  // Rutas de negocio
  app.use('/notifications', notificationRouter);

  // Panel de demo (HTML estático) — se monta ANTES que las rutas de demo
  // para que GET /demo sirva el HTML directamente
  app.use('/demo', express.static(path.join(__dirname, '..', 'public')));
  app.get('/demo', (_req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'demo.html'));
  });

  // Rutas de demo API (protegidas — solo non-prod)
  app.use('/demo', demoRouter);

  // Health check
  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  app.listen(env.PORT, () => {
    console.log(`[Bootstrap] Servidor en http://localhost:${env.PORT}`);
    console.log(`[Bootstrap] Panel de demo en http://localhost:${env.PORT}/demo`);
  });

  // Graceful shutdown
  process.on('SIGTERM', async () => {
    console.log('[Shutdown] Cerrando conexiones...');
    await kafkaProducer.disconnect();
    await pgPool.end();
    process.exit(0);
  });
}

bootstrap().catch(err => {
  console.error('[Bootstrap] Error fatal:', err);
  process.exit(1);
});
