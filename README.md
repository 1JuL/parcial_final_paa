# Banco Dhabi SMS v2.0

Este proyecto implementa el servicio de envío de códigos OTP por SMS para el Banco Dhabi utilizando una arquitectura moderna orientada a eventos.

## Arquitectura y Funcionamiento

El sistema está construido bajo los siguientes patrones:

1. **CQRS (Command Query Responsibility Segregation):**
   - **Command Side (Escritura):** Cuando se solicita enviar un SMS, el registro inicial se guarda en **PostgreSQL** para garantizar consistencia transaccional e integridad.
   - **Query Side (Lectura):** Las consultas del historial de mensajes se realizan desde **MongoDB**, una base de datos optimizada para lectura rápida.
2. **Event-Driven (Apache Kafka):**
   - La comunicación entre el Command Side y el Query Side es asíncrona mediante Kafka. Al guardar en Postgres, se emite un evento en Kafka que un _Consumer_ atrapa en segundo plano para sincronizar (hacer upsert) los datos en MongoDB.
3. **Circuit Breaker (Tolerancia a fallos):**
   - El envío de SMS utiliza **ClickSend** como proveedor principal. Si ClickSend falla de manera consecutiva (5 veces), el circuito se "abre" y el tráfico es desviado instantáneamente al proveedor de respaldo (**Twilio**).
   - Pasado un tiempo de recuperación, el circuito entra en estado `HALF_OPEN`, enviando solo un 33% del tráfico de prueba al primario para confirmar si ya se recuperó antes de restaurarle el 100% de la carga.

---

## Cómo probar con Postman

Puedes interactuar directamente con la API REST del servicio en lugar de usar el panel de Demo. Asegúrate de tener el servidor corriendo (`npm run dev`).

### 1. Enviar un código OTP (Command)

Este endpoint genera el OTP, lo guarda en PostgreSQL, publica el evento en Kafka e intenta mandarlo por el Circuit Breaker.

- **Método:** `POST`
- **URL:** `http://localhost:3000/notifications/otp`
- **Headers:** `Content-Type: application/json`
- **Body (JSON):**
  ```json
  {
    "phone": "+573001234567"
  }
  ```
- **Respuesta Exitosa (202 Accepted):**
  ```json
  {
    "notificationId": "a1b2c3d4-e5f6-7890-1234-56789abcdef0",
    "provider": "CLICKSEND",
    "status": "SENT"
  }
  ```

### 2. Consultar el historial de un número (Query)

Este endpoint lee los datos **directamente desde MongoDB** (Query Side), lo que demuestra que los eventos de Kafka están sincronizando la información correctamente.

- **Método:** `GET`
- **URL:** `http://localhost:3000/notifications/phone/+573001234567`
  _(Nota: Asegúrate de codificar el símbolo `+` en Postman si lo envías por URL, por ejemplo `%2B573001234567`, aunque Postman suele auto-codificarlo)._
- **Respuesta (200 OK):**
  ```json
  [
    {
      "id": "a1b2c3d4-e5f6-7890-1234-56789abcdef0",
      "phone": "+573001234567",
      "status": "SENT",
      "provider": "CLICKSEND",
      "attempts": 1,
      "createdAt": "2026-05-30T05:40:00.000Z",
      "updatedAt": "2026-05-30T05:40:02.000Z"
    }
  ]
  ```

### 3. Consultar el estado del Circuit Breaker

Si quieres saber internamente en qué estado se encuentra el Circuit Breaker (CLOSED, OPEN, o HALF_OPEN) y cuántos fallos lleva acumulados.

- **Método:** `GET`
- **URL:** `http://localhost:3000/notifications/health/cb`
- **Respuesta (200 OK):**
  ```json
  {
    "state": "CLOSED",
    "failureCount": 0,
    "threshold": 5
  }
  ```

### 4. Simular Fallos para probar el Fallback (Twilio)

Como probar el Circuit Breaker desde Postman:

1. Puedes apagar temporalmente el internet, o enviar una petición al endpoint oculto del simulador para "tumbar" ClickSend:
   - `POST http://localhost:3000/demo/provider`
   - Body: `{ "provider": "clicksend", "enabled": false }`
2. Luego, envía 5 peticiones de OTP (`POST /notifications/otp`).
3. Verás que en la 6ta petición, la respuesta cambiará a `"provider": "TWILIO"` y `"status": "FALLBACK_SENT"`.
