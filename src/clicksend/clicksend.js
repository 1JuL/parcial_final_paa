/**
 * Proveedor primario: ClickSend
 *
 * API: REST v3  →  https://rest.clicksend.com/v3/sms/send
 * Docs: https://developers.clicksend.com/docs/messaging/sms/other/send-sms
 *
 * Autenticación: HTTP Basic Auth
 *   username → CLICKSEND_USERNAME   (tu email o usuario de ClickSend)
 *   password → CLICKSEND_API_KEY    (API key del dashboard de ClickSend)
 *
 * Variables de entorno requeridas:
 *   CLICKSEND_USERNAME    — usuario / email de la cuenta ClickSend
 *   CLICKSEND_API_KEY     — API Key del dashboard de ClickSend
 *   CLICKSEND_SENDER_ID   — (opcional) Sender ID o número de origen
 *
 * Modo demo sin credenciales reales:
 *   CLICKSEND_FORCE_FAIL=true  →  simula caída del proveedor
 */

const CLICKSEND_URL = 'https://rest.clicksend.com/v3/sms/send';
const TIMEOUT_MS    = 5_000;  // 5 s máximo esperamos a ClickSend

/**
 * Envía un SMS usando la API v3 de ClickSend.
 *
 * @param {string} to      — número destino en formato internacional (+57300...)
 * @param {string} message — texto del SMS (OTP u otro)
 * @returns {Promise<{
 *   success:    boolean,
 *   provider:   'clicksend',
 *   message_id: string | null,
 *   status:     string,
 * }>}
 * @throws {Error} si la llamada falla (timeout, HTTP error, status no SUCCESS)
 */
async function sendSmsClickSend(to, message) {
  // ── Modo simulación de fallo (demo sin credenciales reales) ───────────────
  if (process.env.CLICKSEND_FORCE_FAIL === 'true') {
    const err  = new Error('ClickSend forzado a fallar (CLICKSEND_FORCE_FAIL=true)');
    err.code   = 'SIMULATED_FAILURE';
    throw err;
  }

  // ── Construir cabecera Basic Auth ─────────────────────────────────────────
  const authHeader = _buildAuthHeader();

  // ── Body del request según la API v3 de ClickSend ─────────────────────────
  // Cada mensaje puede llevar un "from" (Sender ID).
  // Si no se configura CLICKSEND_SENDER_ID se omite y ClickSend usa el default
  // de la cuenta (número compartido del sistema).
  const messagePayload = {
    to,
    body:   message,
    source: 'sdk',  // campo libre; ayuda a identificar el origen en el dashboard
  };

  const senderId = process.env.CLICKSEND_SENDER_ID;
  if (senderId) {
    messagePayload.from = senderId;
  }

  const requestBody = {
    messages: [messagePayload],
  };

  // ── Llamada HTTP con timeout ───────────────────────────────────────────────
  const controller = new AbortController();
  const timer      = setTimeout(() => controller.abort(), TIMEOUT_MS);

  let response;
  try {
    response = await fetch(CLICKSEND_URL, {
      method:  'POST',
      signal:  controller.signal,
      headers: {
        'Content-Type':  'application/json',
        'Authorization': authHeader,
      },
      body: JSON.stringify(requestBody),
    });
  } catch (fetchErr) {
    const isTimeout = fetchErr.name === 'AbortError';
    const err       = new Error(
      isTimeout
        ? `ClickSend timeout (${TIMEOUT_MS}ms)`
        : `ClickSend error de red: ${fetchErr.message}`
    );
    err.code = isTimeout ? 'TIMEOUT' : 'NETWORK_ERROR';
    throw err;
  } finally {
    clearTimeout(timer);
  }

  // ── Parsear respuesta ─────────────────────────────────────────────────────
  let body;
  try {
    body = await response.json();
  } catch {
    const err  = new Error(`ClickSend devolvió respuesta no JSON (HTTP ${response.status})`);
    err.code   = 'INVALID_RESPONSE';
    throw err;
  }

  // HTTP error (4xx / 5xx)
  if (!response.ok) {
    const err  = new Error(
      `ClickSend HTTP ${response.status}: ${body.response_msg ?? 'error desconocido'}`
    );
    err.code   = `HTTP_${response.status}`;
    err.status = response.status;
    throw err;
  }

  // ── Verificar response_code de ClickSend ──────────────────────────────────
  // Incluso con HTTP 200, ClickSend puede retornar response_code !== 'SUCCESS'
  if (body.response_code !== 'SUCCESS') {
    const err  = new Error(`ClickSend response_code: ${body.response_code} — ${body.response_msg}`);
    err.code   = body.response_code;
    throw err;
  }

  // ── Verificar estado individual del mensaje ───────────────────────────────
  // La API retorna data.messages[] con el status por destinatario.
  // Posibles valores: SUCCESS, FAILED, QUEUE, etc.
  const sentMessage = body?.data?.messages?.[0];
  if (!sentMessage) {
    const err  = new Error('ClickSend no retornó datos del mensaje enviado');
    err.code   = 'NO_MESSAGE_DATA';
    throw err;
  }

  // ClickSend a veces retorna HTTP 200 pero con status FAILED por número inválido, etc.
  if (sentMessage.status !== 'SUCCESS' && sentMessage.status !== 'QUEUE') {
    const err  = new Error(
      `ClickSend mensaje status: ${sentMessage.status} — destinatario: ${to}`
    );
    err.code   = `MSG_${sentMessage.status}`;
    throw err;
  }

  // ── Éxito ─────────────────────────────────────────────────────────────────
  return {
    success:    true,
    provider:   'clicksend',
    message_id: sentMessage.message_id ?? null,
    status:     sentMessage.status,
    price:      sentMessage.message_price ?? null,  // útil para monitoreo de costos
  };
}

// ── Helper privado ────────────────────────────────────────────────────────────

function _buildAuthHeader() {
  const username = process.env.CLICKSEND_USERNAME;
  const apiKey   = process.env.CLICKSEND_API_KEY;

  if (!username || !apiKey) {
    throw new Error(
      'CLICKSEND_USERNAME / CLICKSEND_API_KEY no están definidos en .env'
    );
  }

  const token = Buffer.from(`${username}:${apiKey}`).toString('base64');
  return `Basic ${token}`;
}

module.exports = { sendSmsClickSend };