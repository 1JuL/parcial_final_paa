-- migrations/002_create_audit_log.sql

CREATE TABLE audit_log (
  id              BIGSERIAL   PRIMARY KEY,
  notification_id UUID        NOT NULL,
  event_type      TEXT        NOT NULL,   -- 'notification.created' | 'notification.sent' | 'notification.failed'
  provider        TEXT,                   -- 'CLICKSEND' | 'TWILIO' | NULL
  kafka_offset    BIGINT,                 -- Offset del mensaje en Kafka (trazabilidad)
  payload         JSONB       NOT NULL,   -- Copia completa del evento
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_notification_id ON audit_log(notification_id);
CREATE INDEX idx_audit_event_type      ON audit_log(event_type);
