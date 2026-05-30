import { kafka }  from '../../config/kafka';
import { pgPool } from '../../config/database';
import { env }    from '../../config/env';
import { KAFKA_TOPICS } from '../topics';
import { addDemoLog }   from '../../demo/demo.store';

/**
 * Consumer B — Audit Log
 * Escucha: TODOS los topics (created, sent, failed)
 * Acción: Inserta cada evento en la tabla audit_log de PostgreSQL
 */
export async function startAuditConsumer(): Promise<void> {
  const consumer = kafka.consumer({
    groupId: `${env.KAFKA_GROUP_ID}-audit`,
  });

  await consumer.connect();
  // Este consumer escucha TODOS los topics
  await consumer.subscribe({ topics: [...KAFKA_TOPICS], fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString());

      await pgPool.query(
        `INSERT INTO audit_log (notification_id, event_type, provider, kafka_offset, payload)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          event.id,
          topic,
          event.provider ?? null,
          message.offset,
          JSON.stringify(event),
        ],
      );

      addDemoLog('info', `[Audit] Evento registrado: ${topic} offset=${message.offset}`);
    },
  });

  console.log('[Consumer B] Audit consumer iniciado');
}
