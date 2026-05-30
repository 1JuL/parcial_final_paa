// Constantes de nombres de topics — usar siempre estas constantes, nunca strings literales
export const TOPIC_NOTIFICATION_CREATED = 'notification.created';
export const TOPIC_NOTIFICATION_SENT    = 'notification.sent';
export const TOPIC_NOTIFICATION_FAILED  = 'notification.failed';

export const KAFKA_TOPICS = [
  TOPIC_NOTIFICATION_CREATED,
  TOPIC_NOTIFICATION_SENT,
  TOPIC_NOTIFICATION_FAILED,
] as const;
