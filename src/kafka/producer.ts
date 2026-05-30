import { kafkaProducer } from '../config/kafka';
import { NotificationEvent } from '../domain/notification.events';
import { addDemoLog } from '../demo/demo.store';

/**
 * Publica un evento en el topic correspondiente.
 * La key del mensaje es el notificationId para garantizar
 * que todos los eventos del mismo OTP vayan a la misma partición
 * (preserva el orden de eventos por notificación).
 */
export async function publishEvent(event: NotificationEvent): Promise<void> {
  await kafkaProducer.send({
    topic:    event.eventType,
    messages: [
      {
        key:   event.id,
        value: JSON.stringify(event),
      },
    ],
  });
  addDemoLog('info', `[Kafka] Evento publicado: ${event.eventType} (id: ${event.id})`);
}
