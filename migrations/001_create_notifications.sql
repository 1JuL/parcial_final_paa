-- migrations/001_create_notifications.sql

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE notifications_cmd (
  id              UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
  phone           TEXT        NOT NULL,
  otp             TEXT        NOT NULL,
  status          TEXT        NOT NULL DEFAULT 'PENDING'
                              CHECK (status IN ('PENDING','SENT','FAILED','FALLBACK_SENT')),
  provider        TEXT        CHECK (provider IN ('CLICKSEND','TWILIO') OR provider IS NULL),
  attempts        INT         NOT NULL DEFAULT 0,
  error_message   TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índice para consultas por teléfono
CREATE INDEX idx_notifications_phone ON notifications_cmd(phone);

-- Índice para el Retry Consumer: busca registros FAILED eficientemente
CREATE INDEX idx_notifications_status ON notifications_cmd(status)
  WHERE status = 'FAILED';
