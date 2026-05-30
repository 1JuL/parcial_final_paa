import { pgPool }             from '../config/database';
import { circuitBreaker }     from '../circuit-breaker/circuit-breaker';
import { ClickSendProvider }  from '../providers/clicksend.provider';
import { TwilioProvider }     from '../providers/twilio.provider';
import { generateOtp }        from '../utils/otp';
import { publishEvent }       from '../kafka/producer';
import { addDemoLog }         from '../demo/demo.store';
import { SendOtpCommand }     from './send-otp.command';

export interface SendOtpResult {
  notificationId: string;
  provider:       'CLICKSEND' | 'TWILIO';
  status:         'SENT' | 'FALLBACK_SENT';
}

const clicksend = new ClickSendProvider();
const twilio    = new TwilioProvider();

/**
 * Command Handler — orquesta:
 * 1. Guardar en PostgreSQL (estado PENDING)
 * 2. Publicar evento notification.created en Kafka
 * 3. Intentar envío SMS con Circuit Breaker (ClickSend → Twilio)
 * 4. Actualizar PostgreSQL con el resultado
 * 5. Publicar evento notification.sent o notification.failed en Kafka
 */
export async function sendOtpHandler(cmd: SendOtpCommand): Promise<SendOtpResult> {
  const otp = generateOtp();

  // PASO 1: Guardar en PostgreSQL con estado PENDING
  // Se guarda ANTES del envío para nunca perder el registro.
  const { rows } = await pgPool.query<{ id: string }>(
    `INSERT INTO notifications_cmd (phone, otp, status)
     VALUES ($1, $2, 'PENDING')
     RETURNING id`,
    [cmd.phone, otp],
  );
  const notificationId = rows[0].id;
  addDemoLog('info', `[Command] Notificación creada: ${notificationId}`);

  // PASO 2: Publicar evento notification.created en Kafka
  await publishEvent({
    eventType: 'notification.created',
    id:        notificationId,
    phone:     cmd.phone,
    createdAt: new Date().toISOString(),
  });

  try {
    // PASO 3: Enviar SMS con Circuit Breaker
    const { usedProvider } = await circuitBreaker.execute(
      () => clicksend.sendOtp(cmd.phone, otp),
      () => twilio.sendOtp(cmd.phone, otp),
    );

    const providerName: 'CLICKSEND' | 'TWILIO' = usedProvider === 'PRIMARY' ? 'CLICKSEND' : 'TWILIO';
    const status: 'SENT' | 'FALLBACK_SENT'     = usedProvider === 'PRIMARY' ? 'SENT' : 'FALLBACK_SENT';

    // PASO 4: Actualizar PostgreSQL con el resultado
    await pgPool.query(
      `UPDATE notifications_cmd
       SET status = $1, provider = $2, attempts = attempts + 1, updated_at = NOW()
       WHERE id = $3`,
      [status, providerName, notificationId],
    );

    // PASO 5: Publicar evento notification.sent en Kafka
    // Consumer A lo sincronizará a MongoDB. Consumer B lo registrará en audit_log.
    await publishEvent({
      eventType: 'notification.sent',
      id:        notificationId,
      phone:     cmd.phone,
      status,
      provider:  providerName,
      attempts:  1,
      updatedAt: new Date().toISOString(),
    });

    addDemoLog('ok', `[Command] OTP enviado via ${providerName} (${status})`);
    return { notificationId, provider: providerName, status };

  } catch (err) {
    // PASO 4 alternativo: ambos proveedores fallaron
    await pgPool.query(
      `UPDATE notifications_cmd
       SET status = 'FAILED', attempts = attempts + 1,
           error_message = $1, updated_at = NOW()
       WHERE id = $2`,
      [(err as Error).message, notificationId],
    );

    // PASO 5 alternativo: publicar notification.failed
    // Consumer C (Retry) lo capturará e intentará de nuevo.
    await publishEvent({
      eventType:    'notification.failed',
      id:           notificationId,
      phone:        cmd.phone,
      attempts:     1,
      errorMessage: (err as Error).message,
      updatedAt:    new Date().toISOString(),
    });

    addDemoLog('error', `[Command] Ambos proveedores fallaron: ${(err as Error).message}`);
    throw new Error(`Envío fallido por todos los proveedores: ${(err as Error).message}`);
  }
}
