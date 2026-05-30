import { kafka }          from '../../config/kafka';
import { pgPool }         from '../../config/database';
import { env }            from '../../config/env';
import { circuitBreaker } from '../../circuit-breaker/circuit-breaker';
import { ClickSendProvider } from '../../providers/clicksend.provider';
import { TwilioProvider }    from '../../providers/twilio.provider';
import { TOPIC_NOTIFICATION_FAILED } from '../topics';
import { publishEvent }   from '../producer';
import { NotificationFailedEvent } from '../../domain/notification.events';
import { addDemoLog }     from '../../demo/demo.store';

const clicksend = new ClickSendProvider();
const twilio    = new TwilioProvider();

/**
 * Consumer C — Retry Worker
 * Escucha: notification.failed
 * Acción: Espera RETRY_DELAY_MS y reintenta el envío SMS (hasta RETRY_MAX_ATTEMPTS)
 *
 * IMPORTANTE: Si el reintento falla, NO publica un nuevo notification.failed
 * para evitar loops infinitos. El límite RETRY_MAX_ATTEMPTS es el freno.
 */
export async function startRetryConsumer(): Promise<void> {
  const consumer = kafka.consumer({
    groupId: `${env.KAFKA_GROUP_ID}-retry`,
  });

  await consumer.connect();
  await consumer.subscribe({
    topics: [TOPIC_NOTIFICATION_FAILED],
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString()) as NotificationFailedEvent;

      // No reintentar si ya se alcanzó el máximo de intentos
      if (event.attempts >= env.RETRY_MAX_ATTEMPTS) {
        addDemoLog('warn', `[Retry] Max intentos alcanzados para ${event.id}. Abandonando.`);
        return;
      }

      // Esperar antes de reintentar (backoff simple)
      await new Promise(r => setTimeout(r, env.RETRY_DELAY_MS));

      addDemoLog('info', `[Retry] Reintentando envío para ${event.id} (intento ${event.attempts + 1})`);

      // Recuperar el OTP desde PostgreSQL (no se guarda en el evento Kafka por seguridad)
      const { rows } = await pgPool.query<{ otp: string }>(
        'SELECT otp FROM notifications_cmd WHERE id = $1',
        [event.id],
      );

      if (rows.length === 0) {
        addDemoLog('error', `[Retry] Notificación ${event.id} no encontrada en SQL. Skipping.`);
        return;
      }

      const otp = rows[0].otp;

      try {
        const { usedProvider } = await circuitBreaker.execute(
          () => clicksend.sendOtp(event.phone, otp),
          () => twilio.sendOtp(event.phone, otp),
        );

        const providerName = usedProvider === 'PRIMARY' ? 'CLICKSEND' : 'TWILIO';
        const status       = usedProvider === 'PRIMARY' ? 'SENT' : 'FALLBACK_SENT';

        await pgPool.query(
          `UPDATE notifications_cmd
           SET status = $1, provider = $2, attempts = $3, updated_at = NOW()
           WHERE id = $4`,
          [status, providerName, event.attempts + 1, event.id],
        );

        await publishEvent({
          eventType: 'notification.sent',
          id:        event.id,
          phone:     event.phone,
          status,
          provider:  providerName,
          attempts:  event.attempts + 1,
          updatedAt: new Date().toISOString(),
        });

        addDemoLog('ok', `[Retry] Reintento exitoso para ${event.id} via ${providerName}`);
      } catch (retryErr) {
        addDemoLog('error', `[Retry] Reintento fallido para ${event.id}: ${(retryErr as Error).message}`);
        // Si el reintento también falla, NO publicar otro notification.failed → evita loop infinito.
        // El límite de RETRY_MAX_ATTEMPTS controla esto en el próximo mensaje.
      }
    },
  });

  console.log('[Consumer C] Retry consumer iniciado');
}
