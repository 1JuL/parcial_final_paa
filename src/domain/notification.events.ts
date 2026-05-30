// Evento publicado cuando el registro se crea en PostgreSQL (estado PENDING)
export interface NotificationCreatedEvent {
  eventType:  'notification.created';
  id:         string;
  phone:      string;           // No incluir OTP en el evento Kafka (seguridad)
  createdAt:  string;           // ISO 8601
}

// Evento publicado cuando el envío SMS fue exitoso (SENT o FALLBACK_SENT)
export interface NotificationSentEvent {
  eventType:  'notification.sent';
  id:         string;
  phone:      string;
  status:     'SENT' | 'FALLBACK_SENT';
  provider:   'CLICKSEND' | 'TWILIO';
  attempts:   number;
  updatedAt:  string;           // ISO 8601
}

// Evento publicado cuando ambos proveedores fallaron
export interface NotificationFailedEvent {
  eventType:     'notification.failed';
  id:            string;
  phone:         string;
  attempts:      number;
  errorMessage:  string;
  updatedAt:     string;        // ISO 8601
}

export type NotificationEvent =
  | NotificationCreatedEvent
  | NotificationSentEvent
  | NotificationFailedEvent;
