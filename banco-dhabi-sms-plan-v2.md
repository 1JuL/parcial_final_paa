# Plan de Desarrollo — Sistema OTP SMS · Banco Dhabi
## Versión 2.0 — Con Kafka y Panel de Demo

> **Documento de referencia para agentes de IA y desarrolladores.**
> Este archivo es autocontenido. Toda la información necesaria para implementar el sistema está aquí.
> No se requieren documentos adicionales. Versión anterior: `v1.0`.

---

## Tabla de Contenidos

1. [Contexto del negocio](#1-contexto-del-negocio)
2. [Decisión de infraestructura: Cloud vs Local](#2-decisión-de-infraestructura-cloud-vs-local)
3. [Arquitectura general](#3-arquitectura-general)
4. [Por qué Kafka en este sistema](#4-por-qué-kafka-en-este-sistema)
5. [Patrones implementados](#5-patrones-implementados)
6. [Stack tecnológico](#6-stack-tecnológico)
7. [Estructura del proyecto](#7-estructura-del-proyecto)
8. [Variables de entorno](#8-variables-de-entorno)
9. [Esquemas de base de datos](#9-esquemas-de-base-de-datos)
10. [Implementación por fases](#10-implementación-por-fases)
    - [Fase 1 — Configuración e infraestructura](#fase-1--configuración-e-infraestructura)
    - [Fase 2 — Dominio](#fase-2--dominio)
    - [Fase 3 — Circuit Breaker + Demo Store](#fase-3--circuit-breaker--demo-store)
    - [Fase 4 — Proveedores SMS](#fase-4--proveedores-sms)
    - [Fase 5 — Kafka: Producer y Consumers](#fase-5--kafka-producer-y-consumers)
    - [Fase 6 — CQRS: Command Handler](#fase-6--cqrs-command-handler)
    - [Fase 7 — CQRS: Query Handler](#fase-7--cqrs-query-handler)
    - [Fase 8 — API REST + Endpoints de Demo](#fase-8--api-rest--endpoints-de-demo)
    - [Fase 9 — Panel de Demo (HTML)](#fase-9--panel-de-demo-html)
    - [Fase 10 — Punto de entrada](#fase-10--punto-de-entrada)
11. [Docker Compose (Kafka local)](#11-docker-compose-kafka-local)
12. [Dependencias y package.json](#12-dependencias-y-packagejson)
13. [Roadmap de implementación](#13-roadmap-de-implementación)
14. [Reglas y restricciones importantes](#14-reglas-y-restricciones-importantes)

---

## 1. Contexto del negocio

El **Banco Dhabi** necesita un sistema que envíe códigos OTP (One-Time Password) por SMS a sus clientes. El sistema debe ser resistente a fallos del proveedor principal de SMS, y debe poder demostrar visualmente su comportamiento en presentaciones.

**Proveedores de SMS disponibles:**
- **Aldeamo** — proveedor principal (API colombiana).
- **Twilio** — proveedor de fallback automático (API global).

**Requisitos arquitectónicos obligatorios:**
1. **Circuit Breaker** — detectar fallos de Aldeamo y activar Twilio automáticamente.
2. **CQRS** — separar escrituras (PostgreSQL) de lecturas (MongoDB).
3. **Fallback entre proveedores** — si Aldeamo falla, usar Twilio sin intervención manual.
4. **Consistencia eventual** — sincronizar datos de PostgreSQL a MongoDB via Kafka.
5. **Kafka** — bus de eventos central para desacoplar escritura, sincronización, auditoría y reintentos.
6. **Panel de Demo** — interfaz web para activar/desactivar proveedores y forzar estados del Circuit Breaker en tiempo real.

---

## 2. Decisión de infraestructura: Cloud vs Local

**Decisión: CLOUD para bases de datos, LOCAL para Kafka.**

| Servicio | Solución | Motivo |
|---|---|---|
| PostgreSQL | **Supabase** (cloud) | Tier gratuito, SSL incluido, sin configuración |
| MongoDB | **MongoDB Atlas** (cloud) | Tier gratuito M0, sin configuración |
| Kafka | **Docker Compose** (local) | Confluent Cloud tiene costo; para demo/dev, Docker es suficiente y gratuito |

**Servicios cloud seleccionados:**
- **PostgreSQL → Supabase** (`https://supabase.com`) — tier gratuito: 500MB.
- **MongoDB → MongoDB Atlas** (`https://cloud.mongodb.com`) — tier gratuito M0: 512MB.

**Kafka en Docker:**
- Se levanta con `docker-compose up -d` antes de iniciar la app.
- Para producción real, migrar a **Confluent Cloud** o **AWS MSK** cambiando solo `KAFKA_BROKERS` en `.env`.

---

## 3. Arquitectura general

```
+-----------------------+
|     Cliente / App     |
+-----------+-----------+
            |
            | HTTP POST /notifications/otp
            v
+-------------------------------+
|     Notification Service      |   (Node.js + TypeScript + Express)
|          Banco Dhabi          |
+-----------+-------------------+
            |
 +----------+-----------+
 |                      |
 v                      v
COMMAND SIDE         QUERY SIDE
(Escritura SQL)      (Lectura NoSQL)
     |                    |
     v                    v
+----------+        +------------------+
|PostgreSQL|        |     MongoDB      |
| Supabase |        |      Atlas       |
|notifications_cmd| | notifications_read|
+----+-----+        +--------+---------+
     |                       ^
     |                       |
     v                       |
+-----------------------------+
|         KAFKA               |
|                             |
| Topic: notification.created |-----> [Consumer A] Mongo Sync
|                             |       Escribe en MongoDB
| Topic: notification.sent    |
|                             |-----> [Consumer B] Audit Log
| Topic: notification.failed  |       Registra en tabla audit_log (SQL)
|                             |
|                             |-----> [Consumer C] Retry Worker
+-----------------------------+       Reintenta los FAILED

            |
            | Command Handler publica evento
            v
   +-------------------+
   |   Circuit Breaker |
   | CLOSED/OPEN/HALF  |
   +--------+----------+
   CLOSED   |   OPEN
   +--------+--------+
   |                 |
   v                 v
+----------+    +----------+
| Aldeamo  |    | Twilio   |
|(primario)|    |(fallback)|
+----------+    +----------+

            +
            |
            v
+---------------------------+
|   Panel de Demo (HTML)    |   Accesible en http://localhost:3000/demo
|   Servido por Express     |
|  - Toggle Aldeamo ON/OFF  |
|  - Toggle Twilio ON/OFF   |
|  - Forzar estado CB       |
|  - Log de eventos en vivo |
|  - Métricas por proveedor |
+---------------------------+
```

**Flujo completo de un OTP (con Kafka):**

1. Cliente hace `POST /notifications/otp`.
2. Command Handler guarda en PostgreSQL con estado `PENDING`.
3. Command Handler publica evento `notification.created` en Kafka.
4. Circuit Breaker intenta envío por Aldeamo (o Twilio si CB está OPEN).
5. Command Handler actualiza el estado en PostgreSQL (`SENT` / `FALLBACK_SENT` / `FAILED`).
6. Command Handler publica evento `notification.sent` o `notification.failed` en Kafka.
7. **Consumer A (Mongo Sync)** consume `notification.sent/failed` y hace upsert en MongoDB.
8. **Consumer B (Audit)** consume todos los eventos y los registra en la tabla `audit_log`.
9. **Consumer C (Retry)** consume `notification.failed` y reintenta después de un delay.
10. El Panel de Demo puede leer el estado del CB y los logs en tiempo real.

**Flujo de lectura (QUERY):**
1. Cliente hace `GET /notifications/:id`.
2. Query Handler lee desde MongoDB. **Nunca desde PostgreSQL.**

---

## 4. Por qué Kafka en este sistema

Esta sección justifica la decisión técnica. Un agente IA debe leerla antes de proponer cambios arquitectónicos.

### Problema que resuelve (versión anterior vs versión actual)

En la versión 1.0, el Sync Worker usaba `setInterval` para sincronizar PostgreSQL → MongoDB. Eso funcionaba, pero tenía tres limitaciones:

1. **Un solo consumidor:** si el negocio necesitaba alimentar un sistema de auditoría, un servicio de reintentos, o analytics, había que agregar lógica dentro del mismo worker. Esto viola el principio de responsabilidad única.
2. **Acoplamiento temporal:** el worker tenía que estar corriendo exactamente mientras la app estaba viva. Si el worker se reiniciaba, los eventos durante ese tiempo se perdían (o se re-procesaban todos desde cero).
3. **Sin reintentos nativos:** si un upsert en MongoDB fallaba, el worker lo olvidaba en el siguiente ciclo.

### Cómo Kafka resuelve estos problemas

```
Un evento publicado en Kafka → múltiples consumers independientes lo procesan
```

- **Consumer A (Mongo Sync):** procesa `notification.sent` y `notification.failed`. Hace upsert en MongoDB. Si falla, Kafka lo reintenta automáticamente (sin perder el evento).
- **Consumer B (Audit Log):** procesa TODOS los eventos y los guarda en `audit_log` en PostgreSQL. Completamente independiente del Consumer A.
- **Consumer C (Retry Worker):** procesa solo `notification.failed`. Espera N segundos y reintenta el envío SMS.

### Regla para decidir si agregar más consumers en el futuro

Cada vez que aparezca un nuevo requerimiento del tipo "cuando se envíe un OTP, también debería pasar X", la respuesta es agregar un nuevo Consumer. Nunca modificar el Command Handler ni los consumers existentes.

### Lo que Kafka NO aporta aquí

Kafka NO reemplaza al Circuit Breaker. El CB sigue siendo necesario para el fallback en tiempo real (el envío del SMS es sincrónico con el request del cliente). Kafka opera en el plano asíncrono (post-envío).

---

## 5. Patrones implementados

### 5.1 Circuit Breaker

Tres estados:

| Estado | Descripción | Comportamiento |
|---|---|---|
| `CLOSED` | Sistema funcionando | Todas las llamadas van a Aldeamo |
| `OPEN` | Demasiados fallos detectados | Todas las llamadas van directo a Twilio. No se intenta Aldeamo. |
| `HALF_OPEN` | Período de prueba tras `recoveryTimeout` | Se intenta Aldeamo una vez. Si falla → `OPEN`. Si funciona → `CLOSED`. |

Parámetros configurables:
- `CB_FAILURE_THRESHOLD` — fallos consecutivos para abrir el circuito (default: 5).
- `CB_RECOVERY_TIMEOUT_MS` — ms antes de pasar de `OPEN` a `HALF_OPEN` (default: 30000).

### 5.2 CQRS

- **Command Side:** escritura exclusiva en PostgreSQL. Tabla `notifications_cmd`.
- **Query Side:** lectura exclusiva desde MongoDB. Colección `notifications_read`.
- No hay consultas cruzadas en runtime.

### 5.3 Fallback entre proveedores

- El método `circuitBreaker.execute(primary, fallback)` decide qué proveedor usar.
- `primary` → Aldeamo. `fallback` → Twilio.
- El Demo Store permite deshabilitar cualquiera de los dos en tiempo real.

### 5.4 Consistencia eventual (via Kafka)

- El Command Handler publica eventos en Kafka después de cada operación.
- Consumer A consume esos eventos y sincroniza a MongoDB.
- Kafka garantiza entrega "at-least-once". Los consumers usan upsert idempotente.
- La ventana de inconsistencia es el tiempo de procesamiento del Consumer A (usualmente < 1s).

### 5.5 Panel de Demo

- Servido como página HTML estática por Express en `/demo`.
- Se comunica con endpoints REST del mismo servicio (`/demo/*`).
- Permite: toggle de proveedores, forzar estado del CB, ver log de eventos en tiempo real (SSE), ver métricas de envíos.
- Los endpoints `/demo/*` están protegidos por `NODE_ENV !== 'production'`.

---

## 6. Stack tecnológico

| Categoría | Tecnología | Versión mínima | Propósito |
|---|---|---|---|
| Runtime | Node.js | 20.x LTS | Servidor |
| Lenguaje | TypeScript | 5.4 | Tipado estático |
| Framework HTTP | Express | 4.19 | API REST + servidor del panel |
| ORM/Driver SQL | pg (node-postgres) | 8.12 | Conexión a Supabase/PostgreSQL |
| ORM NoSQL | Mongoose | 8.4 | Conexión a MongoDB Atlas |
| Kafka client | kafkajs | 2.2 | Producer y consumers de Kafka |
| HTTP client | Axios | 1.7 | Llamadas a API de Aldeamo |
| SDK SMS | twilio | 5.3 | Llamadas a API de Twilio |
| Validación env | Zod | 3.23 | Validar variables de entorno al arrancar |
| Testing | Vitest | 1.6 | Tests unitarios e integración |
| Dev runner | tsx | 4.15 | Ejecutar TypeScript directamente |
| Infraestructura | Docker + Docker Compose | 24.x | Kafka + Zookeeper en local |

---

## 7. Estructura del proyecto

```
banco-dhabi-sms/
├── src/
│   ├── config/
│   │   ├── database.ts                    # Pool PostgreSQL (Supabase)
│   │   ├── mongo.ts                       # Conexión MongoDB (Atlas)
│   │   ├── kafka.ts                       # Cliente Kafka (producer + admin)
│   │   └── env.ts                         # Validación Zod de env vars
│   │
│   ├── domain/
│   │   ├── notification.entity.ts         # Tipos e interfaces del dominio
│   │   └── notification.events.ts         # Tipos de eventos Kafka
│   │
│   ├── command/                           # CQRS — lado escritura
│   │   ├── send-otp.command.ts            # Tipo del comando
│   │   └── send-otp.handler.ts            # Orquesta: SQL + CB + Kafka publish
│   │
│   ├── query/                             # CQRS — lado lectura
│   │   ├── get-notification.handler.ts    # Lee desde MongoDB
│   │   └── notification-read.repository.ts # Schema Mongoose
│   │
│   ├── providers/
│   │   ├── sms.provider.interface.ts      # Interfaz común
│   │   ├── aldeamo.provider.ts            # Implementación Aldeamo
│   │   └── twilio.provider.ts             # Implementación Twilio
│   │
│   ├── circuit-breaker/
│   │   └── circuit-breaker.ts             # CB + singleton + forceState()
│   │
│   ├── demo/
│   │   └── demo.store.ts                  # Estado mutable para la demo (overrides)
│   │
│   ├── kafka/
│   │   ├── producer.ts                    # Publica eventos
│   │   ├── consumers/
│   │   │   ├── mongo-sync.consumer.ts     # Consumer A: SQL → MongoDB
│   │   │   ├── audit.consumer.ts          # Consumer B: audit_log
│   │   │   └── retry.consumer.ts          # Consumer C: reintentos de FAILED
│   │   └── topics.ts                      # Constantes de nombres de topics
│   │
│   ├── api/
│   │   ├── notification.controller.ts     # Rutas de negocio
│   │   └── demo.controller.ts             # Rutas /demo/* (solo non-prod)
│   │
│   └── main.ts                            # Bootstrap
│
├── public/
│   └── demo.html                          # Panel de demo (HTML puro)
│
├── migrations/
│   ├── 001_create_notifications.sql
│   └── 002_create_audit_log.sql
│
├── docker-compose.yml                     # Kafka + Zookeeper
├── .env.example
├── tsconfig.json
└── package.json
```

---

## 8. Variables de entorno

Crear archivo `.env` en la raíz. **Todas son obligatorias** — la app lanza error y no inicia si falta alguna.

```env
# ── Supabase (PostgreSQL) ─────────────────────────────────────────────────────
# Obtener desde: Supabase Dashboard → Settings → Database → Connection String
# Usar URI con pooler (puerto 6543) para producción
DATABASE_URL=postgresql://postgres.[project-ref]:[password]@aws-0-us-east-1.pooler.supabase.com:6543/postgres

# ── MongoDB Atlas ─────────────────────────────────────────────────────────────
# Obtener desde: Atlas Dashboard → Connect → Connect your application
MONGO_URI=mongodb+srv://[user]:[password]@[cluster].mongodb.net/?retryWrites=true&w=majority

# ── Kafka ─────────────────────────────────────────────────────────────────────
# En local con Docker: localhost:9092
# En producción (Confluent Cloud / AWS MSK): cambiar solo este valor
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=banco-dhabi-sms
KAFKA_GROUP_ID=banco-dhabi-consumers

# ── Aldeamo ───────────────────────────────────────────────────────────────────
ALDEAMO_API_URL=https://api.aldeamo.com/sms/send
ALDEAMO_API_KEY=tu_api_key_de_aldeamo

# ── Twilio ────────────────────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=tu_auth_token_de_twilio
TWILIO_FROM_NUMBER=+15005550006

# ── Circuit Breaker ───────────────────────────────────────────────────────────
CB_FAILURE_THRESHOLD=5          # Fallos para abrir el circuito
CB_RECOVERY_TIMEOUT_MS=30000    # Ms antes de pasar de OPEN a HALF_OPEN

# ── Retry Consumer ────────────────────────────────────────────────────────────
RETRY_DELAY_MS=10000            # Espera antes de reintentar un OTP fallido
RETRY_MAX_ATTEMPTS=3            # Máximo de reintentos por notificación

# ── Servidor ──────────────────────────────────────────────────────────────────
PORT=3000
NODE_ENV=development            # 'production' deshabilita los endpoints /demo/*
```

---

## 9. Esquemas de base de datos

### 9.1 PostgreSQL — Migration 001: tabla `notifications_cmd`

```sql
-- migrations/001_create_notifications.sql

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE notifications_cmd (
  id              UUID        DEFAULT gen_random_uuid() PRIMARY KEY,
  phone           TEXT        NOT NULL,
  otp             TEXT        NOT NULL,
  status          TEXT        NOT NULL DEFAULT 'PENDING'
                              CHECK (status IN ('PENDING','SENT','FAILED','FALLBACK_SENT')),
  provider        TEXT        CHECK (provider IN ('ALDEAMO','TWILIO') OR provider IS NULL),
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
```

**NOTA:** En la versión 2.0 se eliminó la columna `synced_to_mongo`. Ya no se necesita porque Kafka garantiza la entrega del evento al Consumer A. Kafka es el mecanismo de sync, no un flag en SQL.

### 9.2 PostgreSQL — Migration 002: tabla `audit_log`

```sql
-- migrations/002_create_audit_log.sql

CREATE TABLE audit_log (
  id              BIGSERIAL   PRIMARY KEY,
  notification_id UUID        NOT NULL,
  event_type      TEXT        NOT NULL,   -- 'notification.created' | 'notification.sent' | 'notification.failed'
  provider        TEXT,                   -- 'ALDEAMO' | 'TWILIO' | NULL
  kafka_offset    BIGINT,                 -- Offset del mensaje en Kafka (trazabilidad)
  payload         JSONB       NOT NULL,   -- Copia completa del evento
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_notification_id ON audit_log(notification_id);
CREATE INDEX idx_audit_event_type ON audit_log(event_type);
```

### 9.3 Kafka — Topics

| Topic | Producido por | Consumido por | Contenido |
|---|---|---|---|
| `notification.created` | Command Handler | Consumer B (Audit) | `{ id, phone, createdAt }` |
| `notification.sent` | Command Handler | Consumer A (Mongo Sync), Consumer B (Audit) | `{ id, phone, status, provider, attempts }` |
| `notification.failed` | Command Handler | Consumer A (Mongo Sync), Consumer B (Audit), Consumer C (Retry) | `{ id, phone, status, attempts, errorMessage }` |

**Configuración de topics (creados automáticamente al arrancar):**
- `partitions: 3` — permite 3 consumers en paralelo por consumer group.
- `replicationFactor: 1` — suficiente para desarrollo local. Usar 3 en producción.

### 9.4 MongoDB — Colección `notifications_read`

No requiere migration. Mongoose crea la colección en el primer upsert.

**Estructura de un documento:**

```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "phone": "+573001234567",
  "status": "SENT",
  "provider": "ALDEAMO",
  "attempts": 1,
  "createdAt": "2025-01-15T10:30:00.000Z",
  "updatedAt": "2025-01-15T10:30:02.500Z"
}
```

El campo `_id` es el mismo UUID de PostgreSQL. Esto garantiza idempotencia en upserts del Consumer A.

---

## 10. Implementación por fases

---

### Fase 1 — Configuración e infraestructura

#### `src/config/env.ts`

```typescript
import { z } from 'zod';

const schema = z.object({
  DATABASE_URL:           z.string().url(),
  MONGO_URI:              z.string().min(1),
  KAFKA_BROKERS:          z.string().min(1),
  KAFKA_CLIENT_ID:        z.string().min(1),
  KAFKA_GROUP_ID:         z.string().min(1),
  ALDEAMO_API_URL:        z.string().url(),
  ALDEAMO_API_KEY:        z.string().min(1),
  TWILIO_ACCOUNT_SID:     z.string().min(1),
  TWILIO_AUTH_TOKEN:      z.string().min(1),
  TWILIO_FROM_NUMBER:     z.string().min(1),
  CB_FAILURE_THRESHOLD:   z.coerce.number().int().positive().default(5),
  CB_RECOVERY_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
  RETRY_DELAY_MS:         z.coerce.number().int().positive().default(10000),
  RETRY_MAX_ATTEMPTS:     z.coerce.number().int().positive().default(3),
  PORT:                   z.coerce.number().int().positive().default(3000),
  NODE_ENV:               z.enum(['development', 'staging', 'production']).default('development'),
});

export const env = schema.parse(process.env);
```

#### `src/config/database.ts`

```typescript
import { Pool } from 'pg';
import { env } from './env';

export const pgPool = new Pool({
  connectionString: env.DATABASE_URL,
  ssl:              { rejectUnauthorized: false },  // Requerido por Supabase
  max:              10,
  idleTimeoutMillis:    30000,
  connectionTimeoutMillis: 5000,
});

pgPool.on('error', (err) => {
  console.error('[PostgreSQL] Error inesperado en el pool:', err);
});
```

#### `src/config/mongo.ts`

```typescript
import mongoose from 'mongoose';
import { env } from './env';

export async function connectMongo(): Promise<void> {
  await mongoose.connect(env.MONGO_URI, { dbName: 'banco_dhabi' });
  console.log('[MongoDB] Conectado a Atlas');
}
```

#### `src/config/kafka.ts`

```typescript
import { Kafka, Admin, Producer } from 'kafkajs';
import { env } from './env';
import { KAFKA_TOPICS } from '../kafka/topics';

export const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers:  env.KAFKA_BROKERS.split(','),
  // En producción con Confluent Cloud, agregar aquí ssl: true y sasl: { ... }
});

// Producer singleton — compartido por toda la app
export const kafkaProducer: Producer = kafka.producer();

// Admin client — solo para crear topics al arrancar
export async function setupKafkaTopics(): Promise<void> {
  const admin: Admin = kafka.admin();
  await admin.connect();

  await admin.createTopics({
    waitForLeaders: true,
    topics: KAFKA_TOPICS.map(topic => ({
      topic,
      numPartitions:     3,
      replicationFactor: 1,   // Cambiar a 3 en producción
    })),
  });

  await admin.disconnect();
  console.log('[Kafka] Topics creados o verificados:', KAFKA_TOPICS);
}
```

---

### Fase 2 — Dominio

#### `src/domain/notification.entity.ts`

```typescript
export type NotificationStatus =
  | 'PENDING'        // Guardado en SQL, aún no enviado
  | 'SENT'           // Enviado por Aldeamo (proveedor primario)
  | 'FAILED'         // Fallaron ambos proveedores
  | 'FALLBACK_SENT'; // Enviado por Twilio (fallback)

export type SmsProvider = 'ALDEAMO' | 'TWILIO';

// Tal como existe en PostgreSQL (Command Side)
export interface Notification {
  id:           string;
  phone:        string;
  otp:          string;
  status:       NotificationStatus;
  provider:     SmsProvider | null;
  attempts:     number;
  errorMessage: string | null;
  createdAt:    Date;
  updatedAt:    Date;
}

// Tal como existe en MongoDB (Query Side)
// No incluye otp (nunca se expone en lectura)
export interface NotificationReadModel {
  id:        string;
  phone:     string;
  status:    NotificationStatus;
  provider:  SmsProvider | null;
  attempts:  number;
  createdAt: Date;
  updatedAt: Date;
}
```

#### `src/domain/notification.events.ts`

Define la estructura de cada evento publicado en Kafka. Estos tipos son el contrato entre el producer y los consumers. Cambiarlos rompe los consumers.

```typescript
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
  provider:   'ALDEAMO' | 'TWILIO';
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
```

#### `src/kafka/topics.ts`

```typescript
// Constantes de nombres de topics — usar siempre estas constantes, nunca strings literales
export const TOPIC_NOTIFICATION_CREATED = 'notification.created';
export const TOPIC_NOTIFICATION_SENT    = 'notification.sent';
export const TOPIC_NOTIFICATION_FAILED  = 'notification.failed';

export const KAFKA_TOPICS = [
  TOPIC_NOTIFICATION_CREATED,
  TOPIC_NOTIFICATION_SENT,
  TOPIC_NOTIFICATION_FAILED,
] as const;
```

#### `src/utils/otp.ts`

```typescript
import { randomInt } from 'crypto';

// randomInt es criptográficamente seguro
export function generateOtp(): string {
  return String(randomInt(100000, 999999));
}
```

---

### Fase 3 — Circuit Breaker + Demo Store

#### `src/circuit-breaker/circuit-breaker.ts`

```typescript
import { env } from '../config/env';

export type CBState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export class CircuitBreaker {
  private state: CBState = 'CLOSED';
  private failureCount: number = 0;
  private lastFailureTime: number = 0;

  private readonly failureThreshold: number = env.CB_FAILURE_THRESHOLD;
  private readonly recoveryTimeout:  number  = env.CB_RECOVERY_TIMEOUT_MS;

  /**
   * Ejecuta `primary`. Si falla o el circuito está OPEN, ejecuta `fallback`.
   */
  async execute<T>(
    primary:  () => Promise<T>,
    fallback: () => Promise<T>,
  ): Promise<{ result: T; usedProvider: 'PRIMARY' | 'FALLBACK' }> {

    if (this.state === 'OPEN') {
      const elapsed = Date.now() - this.lastFailureTime;
      if (elapsed >= this.recoveryTimeout) {
        this.state = 'HALF_OPEN';
        console.log('[CB] Estado → HALF_OPEN');
      } else {
        const result = await fallback();
        return { result, usedProvider: 'FALLBACK' };
      }
    }

    try {
      const result = await primary();
      this.onSuccess();
      return { result, usedProvider: 'PRIMARY' };
    } catch (primaryError) {
      this.onFailure();
      console.warn('[CB] Primario falló, activando fallback:', (primaryError as Error).message);
      const result = await fallback();
      return { result, usedProvider: 'FALLBACK' };
    }
  }

  private onSuccess(): void {
    this.failureCount = 0;
    if (this.state !== 'CLOSED') {
      this.state = 'CLOSED';
      console.log('[CB] Estado → CLOSED. Sistema recuperado.');
    }
  }

  private onFailure(): void {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    const shouldOpen =
      this.failureCount >= this.failureThreshold ||
      this.state === 'HALF_OPEN';
    if (shouldOpen) {
      this.state = 'OPEN';
      console.error(`[CB] Estado → OPEN tras ${this.failureCount} fallos.`);
    }
  }

  // Método para el panel de demo — permite forzar el estado manualmente
  forceState(newState: CBState): void {
    const prev = this.state;
    this.state = newState;
    if (newState === 'CLOSED') {
      this.failureCount = 0;
    }
    console.log(`[CB][DEMO] Estado forzado: ${prev} → ${newState}`);
  }

  resetFailures(): void {
    this.failureCount = 0;
    console.log('[CB][DEMO] Contador de fallos reseteado a 0');
  }

  getState(): CBState       { return this.state; }
  getFailureCount(): number { return this.failureCount; }
  getThreshold(): number    { return this.failureThreshold; }
}

// Singleton: toda la app usa esta instancia
export const circuitBreaker = new CircuitBreaker();
```

#### `src/demo/demo.store.ts`

Almacena el estado de los overrides de demo en memoria. Es el único lugar donde se persiste este estado. No usar base de datos para esto.

```typescript
export interface ProviderOverride {
  enabled: boolean;   // Si false, el proveedor lanza error inmediatamente
  slow:    boolean;   // Si true, el proveedor simula un timeout (>8s)
}

export interface DemoStore {
  aldeamo: ProviderOverride;
  twilio:  ProviderOverride;
  logs:    DemoLogEntry[];
}

export interface DemoLogEntry {
  timestamp: string;    // ISO 8601
  level:     'info' | 'ok' | 'warn' | 'error';
  message:   string;
}

// Estado inicial: ambos proveedores activos
const store: DemoStore = {
  aldeamo: { enabled: true, slow: false },
  twilio:  { enabled: true, slow: false },
  logs:    [],
};

export function getProviderOverride(provider: 'aldeamo' | 'twilio'): ProviderOverride {
  return store[provider];
}

export function setProviderOverride(
  provider: 'aldeamo' | 'twilio',
  override: Partial<ProviderOverride>,
): void {
  store[provider] = { ...store[provider], ...override };
}

export function addDemoLog(level: DemoLogEntry['level'], message: string): void {
  const entry: DemoLogEntry = {
    timestamp: new Date().toISOString(),
    level,
    message,
  };
  store.logs.push(entry);
  // Mantener solo los últimos 200 logs en memoria
  if (store.logs.length > 200) {
    store.logs.shift();
  }
}

export function getDemoLogs(): DemoLogEntry[] {
  return [...store.logs];
}

export function getDemoStore(): DemoStore {
  return store;
}
```

---

### Fase 4 — Proveedores SMS

#### `src/providers/sms.provider.interface.ts`

```typescript
export interface SmsProvider {
  /**
   * Envía un OTP por SMS.
   * @throws Error si el envío falla (el Circuit Breaker captura esta excepción)
   */
  sendOtp(phone: string, otp: string): Promise<void>;
}
```

#### `src/providers/aldeamo.provider.ts`

```typescript
import axios from 'axios';
import { env } from '../config/env';
import { SmsProvider } from './sms.provider.interface';
import { getProviderOverride, addDemoLog } from '../demo/demo.store';

export class AldeamoProvider implements SmsProvider {
  async sendOtp(phone: string, otp: string): Promise<void> {
    // ── Override de demo ─────────────────────────────────────────────────────
    // Este bloque se ejecuta SIEMPRE (en dev y prod).
    // En prod, los overrides nunca cambian del default (enabled: true, slow: false),
    // así que este bloque es un no-op en producción.
    const override = getProviderOverride('aldeamo');
    if (!override.enabled) {
      addDemoLog('error', '[Aldeamo] Deshabilitado manualmente por demo');
      throw new Error('[DEMO] Aldeamo deshabilitado manualmente');
    }
    if (override.slow) {
      addDemoLog('warn', '[Aldeamo] Simulando lentitud (timeout)');
      // Esperar más que el timeout de axios para provocar el error
      await new Promise(r => setTimeout(r, 10000));
    }
    // ── Llamada real a Aldeamo ────────────────────────────────────────────────
    // NOTA PARA AGENTE: Verificar la estructura exacta del request en https://docs.aldeamo.com
    await axios.post(
      env.ALDEAMO_API_URL,
      {
        to:      phone,
        message: `Tu código OTP del Banco Dhabi es: ${otp}. Válido 5 minutos. No lo compartas.`,
      },
      {
        headers: {
          'Authorization': `Bearer ${env.ALDEAMO_API_KEY}`,
          'Content-Type':  'application/json',
        },
        timeout: 8000,
      },
    );
    addDemoLog('ok', `[Aldeamo] SMS enviado a ${phone}`);
  }
}
```

#### `src/providers/twilio.provider.ts`

```typescript
import twilio from 'twilio';
import { env } from '../config/env';
import { SmsProvider } from './sms.provider.interface';
import { getProviderOverride, addDemoLog } from '../demo/demo.store';

export class TwilioProvider implements SmsProvider {
  private readonly client = twilio(env.TWILIO_ACCOUNT_SID, env.TWILIO_AUTH_TOKEN);

  async sendOtp(phone: string, otp: string): Promise<void> {
    // ── Override de demo ──────────────────────────────────────────────────────
    const override = getProviderOverride('twilio');
    if (!override.enabled) {
      addDemoLog('error', '[Twilio] Deshabilitado manualmente por demo');
      throw new Error('[DEMO] Twilio deshabilitado manualmente');
    }
    if (override.slow) {
      addDemoLog('warn', '[Twilio] Simulando lentitud');
      await new Promise(r => setTimeout(r, 15000));
    }
    // ── Llamada real a Twilio ─────────────────────────────────────────────────
    await this.client.messages.create({
      from: env.TWILIO_FROM_NUMBER,
      to:   phone,
      body: `Tu código OTP del Banco Dhabi es: ${otp}. Válido 5 minutos. No lo compartas.`,
    });
    addDemoLog('ok', `[Twilio] SMS enviado a ${phone}`);
  }
}
```

---

### Fase 5 — Kafka: Producer y Consumers

#### `src/kafka/producer.ts`

```typescript
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
```

#### `src/kafka/consumers/mongo-sync.consumer.ts`

Consumer A: sincroniza eventos de Kafka a MongoDB.

```typescript
import { kafka } from '../../config/kafka';
import { env }   from '../../config/env';
import { NotificationReadModel } from '../../query/notification-read.repository';
import { TOPIC_NOTIFICATION_SENT, TOPIC_NOTIFICATION_FAILED } from '../topics';
import { NotificationSentEvent, NotificationFailedEvent } from '../../domain/notification.events';
import { addDemoLog } from '../../demo/demo.store';

export async function startMongoSyncConsumer(): Promise<void> {
  const consumer = kafka.consumer({
    groupId: `${env.KAFKA_GROUP_ID}-mongo-sync`,
  });

  await consumer.connect();
  await consumer.subscribe({
    topics: [TOPIC_NOTIFICATION_SENT, TOPIC_NOTIFICATION_FAILED],
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString()) as
        NotificationSentEvent | NotificationFailedEvent;

      // Upsert idempotente: si el documento ya existe (por reintento de Kafka),
      // simplemente se actualiza con los mismos datos. Sin efectos secundarios.
      await NotificationReadModel.updateOne(
        { _id: event.id },
        {
          $set: {
            phone:     event.phone,
            status:    event.eventType === 'notification.sent'
                         ? (event as NotificationSentEvent).status
                         : 'FAILED',
            provider:  event.eventType === 'notification.sent'
                         ? (event as NotificationSentEvent).provider
                         : null,
            attempts:  event.attempts,
            updatedAt: new Date(event.updatedAt),
          },
          $setOnInsert: {
            createdAt: new Date(event.updatedAt),
          },
        },
        { upsert: true },
      );

      addDemoLog('info', `[MongoSync] Upsert: ${event.id} (${topic})`);
    },
  });

  console.log('[Consumer A] Mongo Sync consumer iniciado');
}
```

#### `src/kafka/consumers/audit.consumer.ts`

Consumer B: registra todos los eventos en `audit_log`.

```typescript
import { kafka }  from '../../config/kafka';
import { pgPool } from '../../config/database';
import { env }    from '../../config/env';
import { KAFKA_TOPICS } from '../topics';
import { addDemoLog }   from '../../demo/demo.store';

export async function startAuditConsumer(): Promise<void> {
  const consumer = kafka.consumer({
    groupId: `${env.KAFKA_GROUP_ID}-audit`,
  });

  await consumer.connect();
  // Este consumer escucha TODOS los topics
  await consumer.subscribe({ topics: [...KAFKA_TOPICS], fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
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
```

#### `src/kafka/consumers/retry.consumer.ts`

Consumer C: reintenta OTPs fallidos después de un delay.

```typescript
import { kafka }          from '../../config/kafka';
import { pgPool }         from '../../config/database';
import { env }            from '../../config/env';
import { circuitBreaker } from '../../circuit-breaker/circuit-breaker';
import { AldeamoProvider } from '../../providers/aldeamo.provider';
import { TwilioProvider }  from '../../providers/twilio.provider';
import { TOPIC_NOTIFICATION_FAILED } from '../topics';
import { publishEvent }   from '../producer';
import { NotificationFailedEvent } from '../../domain/notification.events';
import { addDemoLog }     from '../../demo/demo.store';

const aldeamo = new AldeamoProvider();
const twilio  = new TwilioProvider();

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
          () => aldeamo.sendOtp(event.phone, otp),
          () => twilio.sendOtp(event.phone, otp),
        );

        const providerName = usedProvider === 'PRIMARY' ? 'ALDEAMO' : 'TWILIO';
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
        // Si el reintento también falla, no publicar otro evento.failed para evitar loops infinitos.
        // El límite de RETRY_MAX_ATTEMPTS controla esto en el próximo ciclo.
      }
    },
  });

  console.log('[Consumer C] Retry consumer iniciado');
}
```

---

### Fase 6 — CQRS: Command Handler

#### `src/command/send-otp.handler.ts`

Orquesta: guardar en SQL → enviar SMS con CB → publicar eventos en Kafka.

```typescript
import { pgPool }          from '../config/database';
import { circuitBreaker }  from '../circuit-breaker/circuit-breaker';
import { AldeamoProvider } from '../providers/aldeamo.provider';
import { TwilioProvider }  from '../providers/twilio.provider';
import { generateOtp }     from '../utils/otp';
import { publishEvent }    from '../kafka/producer';
import { addDemoLog }      from '../demo/demo.store';

export interface SendOtpCommand {
  phone: string;
}

export interface SendOtpResult {
  notificationId: string;
  provider:       'ALDEAMO' | 'TWILIO';
  status:         'SENT' | 'FALLBACK_SENT';
}

const aldeamo = new AldeamoProvider();
const twilio  = new TwilioProvider();

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
      () => aldeamo.sendOtp(cmd.phone, otp),
      () => twilio.sendOtp(cmd.phone, otp),
    );

    const providerName: 'ALDEAMO' | 'TWILIO' = usedProvider === 'PRIMARY' ? 'ALDEAMO' : 'TWILIO';
    const status: 'SENT' | 'FALLBACK_SENT'   = usedProvider === 'PRIMARY' ? 'SENT' : 'FALLBACK_SENT';

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
```

---

### Fase 7 — CQRS: Query Handler

#### `src/query/notification-read.repository.ts`

```typescript
import mongoose, { Schema, Document } from 'mongoose';

export interface NotificationReadDoc extends Document {
  _id:       string;
  phone:     string;
  status:    string;
  provider:  string | null;
  attempts:  number;
  createdAt: Date;
  updatedAt: Date;
}

const schema = new Schema<NotificationReadDoc>(
  {
    _id:      { type: String, required: true },
    phone:    { type: String, required: true, index: true },
    status:   { type: String, required: true },
    provider: { type: String, default: null },
    attempts: { type: Number, default: 0 },
    createdAt: { type: Date },
    updatedAt: { type: Date },
  },
  { _id: false, timestamps: false },
);

export const NotificationReadModel = mongoose.model<NotificationReadDoc>(
  'notifications_read',
  schema,
);
```

#### `src/query/get-notification.handler.ts`

```typescript
import { NotificationReadModel } from './notification-read.repository';
import { NotificationReadModel as NotificationReadType } from '../domain/notification.entity';

/**
 * Lee desde MongoDB. NUNCA desde PostgreSQL.
 *
 * NOTA DE CONSISTENCIA EVENTUAL: puede haber una ventana de tiempo breve
 * (tiempo de procesamiento del Consumer A) en que el registro exista en
 * PostgreSQL pero aún no en MongoDB. En la práctica esto es < 1s.
 * Si el cliente recibe un 404 inmediatamente tras crear el OTP, debe reintentar.
 */
export async function getNotificationByIdHandler(id: string): Promise<NotificationReadType> {
  const doc = await NotificationReadModel.findById(id).lean();

  if (!doc) {
    throw new Error(
      `Notificación ${id} no encontrada. Si acaba de crearse, puede estar sincronizándose (espera ~1s).`,
    );
  }

  return {
    id:        doc._id,
    phone:     doc.phone,
    status:    doc.status as any,
    provider:  doc.provider as any,
    attempts:  doc.attempts,
    createdAt: doc.createdAt,
    updatedAt: doc.updatedAt,
  };
}

export async function getNotificationsByPhoneHandler(phone: string): Promise<NotificationReadType[]> {
  const docs = await NotificationReadModel
    .find({ phone })
    .sort({ createdAt: -1 })
    .limit(20)
    .lean();

  return docs.map(doc => ({
    id:        doc._id,
    phone:     doc.phone,
    status:    doc.status as any,
    provider:  doc.provider as any,
    attempts:  doc.attempts,
    createdAt: doc.createdAt,
    updatedAt: doc.updatedAt,
  }));
}
```

---

### Fase 8 — API REST + Endpoints de Demo

#### `src/api/notification.controller.ts`

```typescript
import { Router, Request, Response } from 'express';
import { sendOtpHandler }            from '../command/send-otp.handler';
import { getNotificationByIdHandler, getNotificationsByPhoneHandler }
  from '../query/get-notification.handler';
import { circuitBreaker }            from '../circuit-breaker/circuit-breaker';

export const notificationRouter = Router();

/**
 * POST /notifications/otp
 * Body: { "phone": "+573001234567" }
 * Response 202: { notificationId, provider, status }
 */
notificationRouter.post('/otp', async (req: Request, res: Response) => {
  const { phone } = req.body as { phone?: string };
  if (!phone) return res.status(400).json({ error: 'Campo "phone" requerido' });

  try {
    const result = await sendOtpHandler({ phone });
    return res.status(202).json(result);
  } catch (err) {
    return res.status(500).json({ error: (err as Error).message });
  }
});

/**
 * GET /notifications/:id
 * Lee desde MongoDB (Query Side)
 */
notificationRouter.get('/:id', async (req: Request, res: Response) => {
  try {
    const n = await getNotificationByIdHandler(req.params.id);
    return res.json(n);
  } catch (err) {
    return res.status(404).json({ error: (err as Error).message });
  }
});

/**
 * GET /notifications/phone/:phone
 * Lee desde MongoDB (Query Side)
 */
notificationRouter.get('/phone/:phone', async (req: Request, res: Response) => {
  try {
    const list = await getNotificationsByPhoneHandler(req.params.phone);
    return res.json(list);
  } catch (err) {
    return res.status(500).json({ error: (err as Error).message });
  }
});

/**
 * GET /notifications/health/cb
 * Estado del Circuit Breaker
 */
notificationRouter.get('/health/cb', (_req, res) => {
  res.json({
    state:        circuitBreaker.getState(),
    failureCount: circuitBreaker.getFailureCount(),
    threshold:    circuitBreaker.getThreshold(),
  });
});
```

#### `src/api/demo.controller.ts`

Todos los endpoints aquí están protegidos. Solo funcionan si `NODE_ENV !== 'production'`.

```typescript
import { Router, Request, Response, NextFunction } from 'express';
import { circuitBreaker, CBState }  from '../circuit-breaker/circuit-breaker';
import {
  setProviderOverride,
  getDemoLogs,
  getDemoStore,
  addDemoLog,
} from '../demo/demo.store';
import { sendOtpHandler } from '../command/send-otp.handler';
import { env } from '../config/env';

export const demoRouter = Router();

// Middleware: bloquear en producción
function demoOnly(_req: Request, res: Response, next: NextFunction): void {
  if (env.NODE_ENV === 'production') {
    res.status(403).json({ error: 'Endpoints de demo no disponibles en producción' });
    return;
  }
  next();
}

demoRouter.use(demoOnly);

/**
 * GET /demo/status
 * Estado completo del sistema para el panel
 */
demoRouter.get('/status', (_req, res) => {
  const store = getDemoStore();
  res.json({
    circuitBreaker: {
      state:        circuitBreaker.getState(),
      failureCount: circuitBreaker.getFailureCount(),
      threshold:    circuitBreaker.getThreshold(),
    },
    providers: {
      aldeamo: store.aldeamo,
      twilio:  store.twilio,
    },
  });
});

/**
 * POST /demo/provider
 * Body: { "provider": "aldeamo" | "twilio", "enabled": boolean, "slow": boolean }
 * Activa o desactiva un proveedor para la demo
 */
demoRouter.post('/provider', (req: Request, res: Response) => {
  const { provider, enabled, slow } = req.body as {
    provider: 'aldeamo' | 'twilio';
    enabled?: boolean;
    slow?:    boolean;
  };

  if (!['aldeamo', 'twilio'].includes(provider)) {
    return res.status(400).json({ error: 'provider debe ser "aldeamo" o "twilio"' });
  }

  const update: { enabled?: boolean; slow?: boolean } = {};
  if (enabled !== undefined) update.enabled = enabled;
  if (slow    !== undefined) update.slow    = slow;

  setProviderOverride(provider, update);
  addDemoLog(
    enabled === false ? 'error' : 'info',
    `[Demo] Proveedor ${provider} → enabled=${enabled ?? '–'}, slow=${slow ?? '–'}`,
  );

  res.json({ provider, ...update });
});

/**
 * POST /demo/cb/force
 * Body: { "state": "CLOSED" | "OPEN" | "HALF_OPEN" }
 * Fuerza el estado del Circuit Breaker
 */
demoRouter.post('/cb/force', (req: Request, res: Response) => {
  const { state } = req.body as { state: CBState };
  if (!['CLOSED', 'OPEN', 'HALF_OPEN'].includes(state)) {
    return res.status(400).json({ error: 'state debe ser CLOSED, OPEN o HALF_OPEN' });
  }
  circuitBreaker.forceState(state);
  addDemoLog(
    state === 'OPEN' ? 'error' : state === 'HALF_OPEN' ? 'warn' : 'ok',
    `[Demo] Circuit Breaker forzado a ${state}`,
  );
  res.json({ state });
});

/**
 * POST /demo/cb/reset
 * Resetea el contador de fallos del CB
 */
demoRouter.post('/cb/reset', (_req, res) => {
  circuitBreaker.resetFailures();
  addDemoLog('info', '[Demo] Contador de fallos del CB reseteado a 0');
  res.json({ failureCount: 0 });
});

/**
 * POST /demo/send
 * Body: { "phone": string }
 * Envía un OTP de prueba (para usar desde el panel)
 */
demoRouter.post('/send', async (req: Request, res: Response) => {
  const { phone } = req.body as { phone?: string };
  const target = phone || '+573001234567';
  try {
    const result = await sendOtpHandler({ phone: target });
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

/**
 * GET /demo/logs
 * Retorna los últimos 200 logs del demo store
 */
demoRouter.get('/logs', (_req, res) => {
  res.json(getDemoLogs());
});

/**
 * GET /demo/logs/stream  (Server-Sent Events)
 * Permite al panel recibir logs en tiempo real sin polling
 */
demoRouter.get('/logs/stream', (req: Request, res: Response) => {
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  const interval = setInterval(() => {
    const logs = getDemoLogs().slice(-10);
    res.write(`data: ${JSON.stringify(logs)}\n\n`);
  }, 1000);

  req.on('close', () => clearInterval(interval));
});
```

---

### Fase 9 — Panel de Demo (HTML)

Crear el archivo `public/demo.html`. Express lo sirve estáticamente en `/demo`.

El panel se comunica con los endpoints `/demo/*` del mismo servidor. No requiere build ni bundler.

```html
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Panel de Demo — Banco Dhabi SMS</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #f5f5f5;
      color: #1a1a1a;
      padding: 2rem;
      font-size: 14px;
    }
    h1 { font-size: 20px; font-weight: 600; margin-bottom: 1.5rem; }
    h2 { font-size: 14px; font-weight: 600; color: #555; text-transform: uppercase;
         letter-spacing: 0.05em; margin-bottom: 0.75rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1rem; margin-bottom: 1.5rem; }
    .card { background: white; border-radius: 10px; padding: 1.25rem;
            border: 1px solid #e5e5e5; }
    .row { display: flex; justify-content: space-between; align-items: center;
           padding: 0.6rem 0; border-bottom: 1px solid #f0f0f0; }
    .row:last-child { border-bottom: none; }

    /* Toggle switch */
    .toggle { position: relative; width: 42px; height: 24px; cursor: pointer; }
    .toggle input { display: none; }
    .toggle-track { position: absolute; inset: 0; border-radius: 12px;
                    background: #d1d5db; transition: 0.2s; }
    .toggle input:checked ~ .toggle-track { background: #16a34a; }
    .toggle-thumb { position: absolute; width: 18px; height: 18px; border-radius: 50%;
                    background: white; top: 3px; left: 3px; transition: 0.2s;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.2); }
    .toggle input:checked ~ .toggle-thumb { transform: translateX(18px); }

    /* Estado badges */
    .badge { display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 10px;
             border-radius: 99px; }
    .badge-closed  { background: #dcfce7; color: #166534; }
    .badge-open    { background: #fee2e2; color: #991b1b; }
    .badge-half    { background: #fef9c3; color: #854d0e; }
    .badge-up      { background: #dcfce7; color: #166534; }
    .badge-down    { background: #fee2e2; color: #991b1b; }

    /* Botones */
    .btn { display: inline-block; padding: 7px 14px; border-radius: 7px; font-size: 13px;
           font-weight: 500; cursor: pointer; border: 1px solid #d1d5db;
           background: white; transition: 0.15s; }
    .btn:hover  { background: #f9fafb; }
    .btn:active { transform: scale(0.97); }
    .btn-red    { border-color: #fca5a5; color: #b91c1c; }
    .btn-red:hover    { background: #fee2e2; }
    .btn-amber  { border-color: #fcd34d; color: #92400e; }
    .btn-amber:hover  { background: #fef9c3; }
    .btn-green  { border-color: #86efac; color: #166534; }
    .btn-green:hover  { background: #dcfce7; }
    .btn-group  { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 0.75rem; }

    /* Barra de fallos */
    .fail-track { height: 6px; border-radius: 3px; background: #e5e7eb; margin-top: 8px; overflow: hidden; }
    .fail-fill  { height: 100%; border-radius: 3px; background: #16a34a;
                  transition: width 0.3s, background 0.3s; }
    .fail-fill.danger { background: #dc2626; }

    /* CB diagram */
    .cb-states { display: flex; align-items: center; gap: 6px; margin: 0.75rem 0; }
    .cb-node { padding: 5px 14px; border-radius: 7px; font-size: 12px; font-weight: 600;
               opacity: 0.35; transition: 0.2s; }
    .cb-node.active { opacity: 1; }
    .cb-closed { background: #dcfce7; color: #166534; }
    .cb-open   { background: #fee2e2; color: #991b1b; }
    .cb-half   { background: #fef9c3; color: #854d0e; }
    .cb-arrow  { color: #9ca3af; font-size: 16px; }

    /* Log box */
    .log-box { background: #1a1a2e; color: #e2e8f0; border-radius: 10px; padding: 1rem;
               font-family: 'Menlo', 'Courier New', monospace; font-size: 12px;
               max-height: 260px; overflow-y: auto; line-height: 1.8; }
    .log-ok    { color: #86efac; }
    .log-error { color: #fca5a5; }
    .log-warn  { color: #fde68a; }
    .log-info  { color: #93c5fd; }
    .log-time  { color: #6b7280; margin-right: 8px; }

    /* Métricas */
    .metrics { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-top: 0.75rem; }
    .metric { background: #f9fafb; border-radius: 8px; padding: 0.75rem; text-align: center; }
    .metric-value { font-size: 24px; font-weight: 700; }
    .metric-label { font-size: 11px; color: #6b7280; margin-top: 2px; }

    /* Input */
    input[type=text] { border: 1px solid #d1d5db; border-radius: 7px; padding: 7px 12px;
                       font-size: 13px; width: 100%; outline: none; background: white; }
    input[type=text]:focus { border-color: #6366f1; box-shadow: 0 0 0 3px rgba(99,102,241,0.1); }

    .section { margin-bottom: 1.5rem; }
    small { color: #9ca3af; font-size: 11px; }
  </style>
</head>
<body>

<h1>🏦 Banco Dhabi — Panel de Demo Circuit Breaker</h1>

<div class="grid">

  <!-- ── Circuit Breaker ── -->
  <div class="card">
    <h2>Circuit Breaker</h2>
    <div class="cb-states">
      <div id="node-closed" class="cb-node cb-closed active">CLOSED</div>
      <span class="cb-arrow">→</span>
      <div id="node-open"   class="cb-node cb-open">OPEN</div>
      <span class="cb-arrow">→</span>
      <div id="node-half"   class="cb-node cb-half">HALF-OPEN</div>
    </div>
    <div class="row">
      <span>Estado actual</span>
      <span id="cb-badge" class="badge badge-closed">CLOSED</span>
    </div>
    <div class="row">
      <span>Fallos consecutivos</span>
      <span id="cb-fails">0 / 5</span>
    </div>
    <div class="fail-track"><div id="fail-bar" class="fail-fill" style="width:0%"></div></div>
    <div class="btn-group">
      <button class="btn btn-green"  onclick="forceCB('CLOSED')">Forzar CLOSED</button>
      <button class="btn btn-red"    onclick="forceCB('OPEN')">Forzar OPEN</button>
      <button class="btn btn-amber"  onclick="forceCB('HALF_OPEN')">Forzar HALF-OPEN</button>
      <button class="btn"            onclick="resetCB()">Reset fallos</button>
    </div>
  </div>

  <!-- ── Proveedores ── -->
  <div class="card">
    <h2>Proveedores SMS</h2>

    <div class="row">
      <div>
        <strong>Aldeamo</strong>
        <small style="display:block">Primario · API Colombia</small>
      </div>
      <span id="tag-aldeamo" class="badge badge-up">activo</span>
    </div>
    <div class="row">
      <span>Habilitado</span>
      <label class="toggle">
        <input type="checkbox" id="chk-aldeamo-enabled" checked
               onchange="setProvider('aldeamo','enabled',this.checked)">
        <span class="toggle-track"></span>
        <span class="toggle-thumb"></span>
      </label>
    </div>
    <div class="row">
      <span>Simular lentitud (timeout)</span>
      <label class="toggle">
        <input type="checkbox" id="chk-aldeamo-slow"
               onchange="setProvider('aldeamo','slow',this.checked)">
        <span class="toggle-track"></span>
        <span class="toggle-thumb"></span>
      </label>
    </div>

    <div style="height:1px;background:#f0f0f0;margin:12px 0"></div>

    <div class="row">
      <div>
        <strong>Twilio</strong>
        <small style="display:block">Fallback · API Global</small>
      </div>
      <span id="tag-twilio" class="badge badge-up">activo</span>
    </div>
    <div class="row">
      <span>Habilitado</span>
      <label class="toggle">
        <input type="checkbox" id="chk-twilio-enabled" checked
               onchange="setProvider('twilio','enabled',this.checked)">
        <span class="toggle-track"></span>
        <span class="toggle-thumb"></span>
      </label>
    </div>
    <div class="row">
      <span>Simular lentitud (timeout)</span>
      <label class="toggle">
        <input type="checkbox" id="chk-twilio-slow"
               onchange="setProvider('twilio','slow',this.checked)">
        <span class="toggle-track"></span>
        <span class="toggle-thumb"></span>
      </label>
    </div>
  </div>

</div>

<!-- ── Simulador de envío ── -->
<div class="card section">
  <h2>Simulador de envío OTP</h2>
  <div style="display:flex;gap:10px;margin:0.75rem 0">
    <input type="text" id="phone-input" value="+573001234567" placeholder="Número de teléfono">
    <button class="btn btn-green" style="white-space:nowrap" onclick="sendOtp()">Enviar OTP</button>
    <button class="btn" style="white-space:nowrap" onclick="sendBatch()">Enviar x5</button>
  </div>
  <div class="metrics">
    <div class="metric">
      <div class="metric-value" id="m-total">0</div>
      <div class="metric-label">Total enviados</div>
    </div>
    <div class="metric">
      <div class="metric-value" id="m-aldeamo" style="color:#16a34a">0</div>
      <div class="metric-label">Via Aldeamo</div>
    </div>
    <div class="metric">
      <div class="metric-value" id="m-twilio" style="color:#d97706">0</div>
      <div class="metric-label">Via Twilio</div>
    </div>
  </div>
</div>

<!-- ── Log en tiempo real ── -->
<div class="card section">
  <h2>Log de eventos en tiempo real <small style="margin-left:6px;color:#9ca3af">SSE</small></h2>
  <div class="log-box" id="log-box">
    <span class="log-info"><span class="log-time">--:--:--</span>Conectando al stream de eventos...</span>
  </div>
</div>

<script>
const metrics = { total: 0, aldeamo: 0, twilio: 0 };

// ── Estado del CB ────────────────────────────────────────────────────────────
async function refreshStatus() {
  try {
    const r = await fetch('/demo/status');
    const d = await r.json();
    updateCBDisplay(d.circuitBreaker);
    updateProviderTags(d.providers);
  } catch(e) { /* servidor no disponible aún */ }
}

function updateCBDisplay(cb) {
  const nodes = { CLOSED:'node-closed', OPEN:'node-open', HALF_OPEN:'node-half' };
  Object.entries(nodes).forEach(([s, id]) => {
    document.getElementById(id).classList.toggle('active', s === cb.state);
  });
  const badge = document.getElementById('cb-badge');
  badge.textContent = cb.state;
  badge.className = 'badge ' + {
    CLOSED:'badge-closed', OPEN:'badge-open', HALF_OPEN:'badge-half'
  }[cb.state];
  const pct = Math.min((cb.failureCount / cb.threshold) * 100, 100);
  document.getElementById('cb-fails').textContent = `${cb.failureCount} / ${cb.threshold}`;
  const bar = document.getElementById('fail-bar');
  bar.style.width = pct + '%';
  bar.className = 'fail-fill' + (pct >= 80 ? ' danger' : '');
}

function updateProviderTags(providers) {
  ['aldeamo','twilio'].forEach(p => {
    const tag = document.getElementById(`tag-${p}`);
    const on  = providers[p].enabled;
    tag.textContent = on ? 'activo' : 'caído';
    tag.className   = 'badge ' + (on ? 'badge-up' : 'badge-down');
    const slow = providers[p].slow;
    document.getElementById(`chk-${p}-enabled`).checked = on;
    document.getElementById(`chk-${p}-slow`).checked    = slow;
  });
}

// ── Acciones de demo ─────────────────────────────────────────────────────────
async function forceCB(state) {
  await fetch('/demo/cb/force', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ state }),
  });
  await refreshStatus();
}

async function resetCB() {
  await fetch('/demo/cb/reset', { method: 'POST' });
  await refreshStatus();
}

async function setProvider(provider, field, value) {
  const body = { provider };
  body[field] = value;
  await fetch('/demo/provider', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  await refreshStatus();
}

// ── Envío de OTP ─────────────────────────────────────────────────────────────
async function sendOtp() {
  const phone = document.getElementById('phone-input').value || '+573001234567';
  metrics.total++;
  document.getElementById('m-total').textContent = metrics.total;

  const r = await fetch('/demo/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ phone }),
  });
  const d = await r.json();

  if (d.provider === 'ALDEAMO') {
    metrics.aldeamo++;
    document.getElementById('m-aldeamo').textContent = metrics.aldeamo;
  } else if (d.provider === 'TWILIO') {
    metrics.twilio++;
    document.getElementById('m-twilio').textContent = metrics.twilio;
  }
  await refreshStatus();
}

async function sendBatch() {
  for (let i = 0; i < 5; i++) {
    await sendOtp();
    await new Promise(r => setTimeout(r, 300));
  }
}

// ── SSE Log stream ─────────────────────────────────────────────────────────
const logBox = document.getElementById('log-box');
let lastLogCount = 0;

const evtSource = new EventSource('/demo/logs/stream');
evtSource.onmessage = (e) => {
  const logs = JSON.parse(e.data);
  if (logs.length === lastLogCount) return;
  lastLogCount = logs.length;

  logBox.innerHTML = logs.map(l => {
    const t = new Date(l.timestamp).toTimeString().slice(0,8);
    return `<div><span class="log-time">${t}</span><span class="log-${l.level}">${l.message}</span></div>`;
  }).join('');
  logBox.scrollTop = logBox.scrollHeight;
};
evtSource.onerror = () => {
  console.warn('SSE desconectado, reintentando...');
};

// Refresh de estado cada 2s
setInterval(refreshStatus, 2000);
refreshStatus();
</script>
</body>
</html>
```

---

### Fase 10 — Punto de entrada

#### `src/main.ts`

```typescript
import 'dotenv/config';
import express                 from 'express';
import path                    from 'path';
import { pgPool }              from './config/database';
import { connectMongo }        from './config/mongo';
import { setupKafkaTopics, kafkaProducer } from './config/kafka';
import { notificationRouter }  from './api/notification.controller';
import { demoRouter }          from './api/demo.controller';
import { startMongoSyncConsumer } from './kafka/consumers/mongo-sync.consumer';
import { startAuditConsumer }     from './kafka/consumers/audit.consumer';
import { startRetryConsumer }     from './kafka/consumers/retry.consumer';
import { env }                 from './config/env';

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

  // Rutas de demo (protegidas — solo non-prod)
  app.use('/demo', demoRouter);

  // Panel de demo (HTML estático)
  app.use('/demo', express.static(path.join(__dirname, '..', 'public')));
  // Ruta directa al panel: GET /demo → public/demo.html
  app.get('/demo', (_req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'demo.html'));
  });

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
```

---

## 11. Docker Compose (Kafka local)

Crear `docker-compose.yml` en la raíz del proyecto.

```yaml
version: '3.9'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    container_name: banco-dhabi-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: banco-dhabi-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"   # Los topics los crea setupKafkaTopics()
      KAFKA_LOG_RETENTION_HOURS: 24
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: banco-dhabi-kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

**Comandos:**

```bash
# Levantar Kafka (ejecutar antes de npm run dev)
docker-compose up -d

# Ver logs de Kafka
docker-compose logs -f kafka

# Acceder a la UI de Kafka (explorar topics y mensajes)
# http://localhost:8080

# Detener todo
docker-compose down
```

---

## 12. Dependencias y package.json

```json
{
  "name": "banco-dhabi-sms",
  "version": "2.0.0",
  "description": "Sistema OTP SMS con Kafka, Circuit Breaker, CQRS, fallback y panel de demo",
  "main": "dist/main.js",
  "scripts": {
    "dev":        "tsx watch src/main.ts",
    "build":      "tsc",
    "start":      "node dist/main.js",
    "test":       "vitest",
    "kafka:up":   "docker-compose up -d",
    "kafka:down": "docker-compose down",
    "migrate":    "psql $DATABASE_URL -f migrations/001_create_notifications.sql && psql $DATABASE_URL -f migrations/002_create_audit_log.sql"
  },
  "dependencies": {
    "axios":    "^1.7.0",
    "dotenv":   "^16.4.0",
    "express":  "^4.19.0",
    "kafkajs":  "^2.2.4",
    "mongoose": "^8.4.0",
    "pg":       "^8.12.0",
    "twilio":   "^5.3.0",
    "zod":      "^3.23.0"
  },
  "devDependencies": {
    "@types/express": "^4.17.0",
    "@types/node":    "^20.0.0",
    "@types/pg":      "^8.11.0",
    "tsx":            "^4.15.0",
    "typescript":     "^5.4.0",
    "vitest":         "^1.6.0"
  }
}
```

#### `tsconfig.json`

```json
{
  "compilerOptions": {
    "target":           "ES2022",
    "module":           "CommonJS",
    "lib":              ["ES2022"],
    "outDir":           "./dist",
    "rootDir":          "./src",
    "strict":           true,
    "esModuleInterop":  true,
    "skipLibCheck":     true,
    "resolveJsonModule": true,
    "forceConsistentCasingInFileNames": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

---

## 13. Roadmap de implementación

| Fase | Tarea | Archivos principales | Duración estimada |
|---|---|---|---|
| 1 | Setup Supabase + Atlas. Levantar Kafka con Docker. Instalar dependencias. | `docker-compose.yml`, `package.json`, `.env` | 1 día |
| 2 | Ejecutar migrations SQL. Implementar dominio, eventos y utils. | `001_*.sql`, `002_*.sql`, `notification.entity.ts`, `notification.events.ts`, `topics.ts` | 1 día |
| 3 | Implementar Circuit Breaker + Demo Store con tests unitarios. | `circuit-breaker.ts`, `demo.store.ts` | 1 día |
| 4 | Implementar proveedores SMS con soporte de overrides de demo. | `aldeamo.provider.ts`, `twilio.provider.ts` | 1 día |
| 5 | Implementar Kafka producer + los 3 consumers. | `producer.ts`, `mongo-sync.consumer.ts`, `audit.consumer.ts`, `retry.consumer.ts` | 2 días |
| 6 | Implementar CQRS Command Handler (integra todo: SQL + CB + Kafka). | `send-otp.handler.ts` | 1 día |
| 7 | Implementar CQRS Query Handler + repositorio MongoDB. | `get-notification.handler.ts`, `notification-read.repository.ts` | 1 día |
| 8 | Implementar API REST + endpoints de demo + panel HTML. | `notification.controller.ts`, `demo.controller.ts`, `demo.html` | 2 días |
| 9 | Tests de integración end-to-end + ajustes. | `*.test.ts` | 1 día |

**Total estimado: 11 días de desarrollo.**

---

## 14. Reglas y restricciones importantes

Un agente IA que trabaje en este código debe leer esta sección antes de hacer cualquier cambio.

### Separación estricta de CQRS

- **PROHIBIDO** leer de PostgreSQL desde el Query Handler.
- **PROHIBIDO** escribir en MongoDB directamente desde el Command Handler.
- Solo el Consumer A (mongo-sync) escribe en MongoDB.
- Toda lectura va a MongoDB. Toda escritura inicial va a PostgreSQL.

### Kafka es el único canal de sync SQL → MongoDB

- No hay Sync Worker con `setInterval`. Fue eliminado en v2.0.
- La sincronización ocurre exclusivamente a través de los eventos Kafka consumidos por Consumer A.
- Si se necesita re-sincronizar un registro, publicar el evento manualmente en el topic correspondiente.

### El OTP nunca viaja por Kafka

- Los eventos Kafka contienen `phone`, `status`, `provider`, `attempts`, pero **nunca el campo `otp`**.
- El OTP solo existe en la tabla `notifications_cmd` de PostgreSQL.
- Si el Consumer C (retry) necesita el OTP, lo recupera con una query SQL `SELECT otp FROM notifications_cmd WHERE id = $1`.

### El Circuit Breaker es un singleton

- `circuit-breaker.ts` exporta `const circuitBreaker = new CircuitBreaker()`.
- **PROHIBIDO** instanciar `new CircuitBreaker()` fuera de ese archivo.

### Los endpoints `/demo/*` son solo para non-producción

- El middleware `demoOnly` en `demo.controller.ts` bloquea todas las rutas `/demo/*` si `NODE_ENV === 'production'`.
- **NUNCA** eliminar ese middleware.
- Los overrides del `demo.store.ts` arrancan con `enabled: true` para ambos proveedores, así que en producción (donde nadie llama a los endpoints de demo) los proveedores siempre funcionan normalmente.

### Idempotencia en Consumer A

- El upsert en MongoDB usa `updateOne` con `upsert: true`.
- Kafka garantiza "at-least-once delivery". El mismo evento puede llegar dos veces.
- El upsert es idempotente: dos escrituras del mismo evento producen el mismo resultado final.

### Evitar loops infinitos en Consumer C (Retry)

- El Consumer C verifica `event.attempts >= RETRY_MAX_ATTEMPTS` antes de reintentar.
- Si el reintento falla, **no** publica un nuevo evento `notification.failed`. El límite de intentos es el freno.
- Si se publicara un nuevo `notification.failed` en cada fallo del retry, se crearía un loop infinito.

### Orden de bootstrap en `main.ts`

El orden de arranque es obligatorio y no debe alterarse:
1. PostgreSQL
2. MongoDB
3. Kafka topics + producer
4. Consumers
5. Express

Si los consumers arrancan antes que el producer, pueden intentar commitear offsets antes de que la conexión esté lista y lanzar errores.

### SSL en Supabase

- `ssl: { rejectUnauthorized: false }` es requerido en el Pool de `pg`.
- Sin esto, la conexión falla con error de certificado TLS.

### Kafka en producción

- Para producción, cambiar `KAFKA_BROKERS` a los brokers de Confluent Cloud o AWS MSK.
- Agregar en `src/config/kafka.ts` la configuración `ssl: true` y `sasl: { mechanism, username, password }`.
- No cambiar nada más — el resto del código es agnóstico al broker.

---

*Fin del documento. Versión 2.0 — Sistema OTP SMS Banco Dhabi con Kafka y Panel de Demo.*
