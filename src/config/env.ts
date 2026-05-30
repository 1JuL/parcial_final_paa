import { z } from 'zod';

const schema = z.object({
  DATABASE_URL:            z.string().url(),
  MONGO_URI:               z.string().min(1),
  KAFKA_BROKERS:           z.string().min(1),
  KAFKA_CLIENT_ID:         z.string().min(1),
  KAFKA_GROUP_ID:          z.string().min(1),
  CLICKSEND_USERNAME:      z.string().min(1),
  CLICKSEND_API_KEY:       z.string().min(1),
  TWILIO_ACCOUNT_SID:      z.string().min(1),
  TWILIO_AUTH_TOKEN:       z.string().min(1),
  TWILIO_FROM_NUMBER:      z.string().min(1),
  CB_FAILURE_THRESHOLD:    z.coerce.number().int().positive().default(5),
  CB_RECOVERY_TIMEOUT_MS:  z.coerce.number().int().positive().default(30000),
  RETRY_DELAY_MS:          z.coerce.number().int().positive().default(10000),
  RETRY_MAX_ATTEMPTS:      z.coerce.number().int().positive().default(3),
  PORT:                    z.coerce.number().int().positive().default(3000),
  NODE_ENV:                z.enum(['development', 'staging', 'production']).default('development'),
});

export const env = schema.parse(process.env);
