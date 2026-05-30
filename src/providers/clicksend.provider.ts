import axios from 'axios';
import { env } from '../config/env';
import { SmsProvider } from './sms.provider.interface';
import { getProviderOverride, addDemoLog } from '../demo/demo.store';

/**
 * ClickSend SMS Provider — Proveedor primario
 *
 * Documentación: https://developers.clicksend.com/docs/rest/v3/#send-sms
 * Autenticación: HTTP Basic Auth (username:api_key) en Base64
 * Endpoint: POST https://rest.clicksend.com/v3/sms/send
 *
 * Registro gratuito en: https://www.clicksend.com/signup
 * (Incluye créditos de prueba sin tarjeta de crédito)
 */
export class ClickSendProvider implements SmsProvider {
  private readonly baseUrl = 'https://rest.clicksend.com/v3/sms/send';
  private readonly authHeader: string;

  constructor() {
    // Basic Auth = base64(username:api_key)
    const credentials = Buffer.from(
      `${env.CLICKSEND_USERNAME}:${env.CLICKSEND_API_KEY}`,
    ).toString('base64');
    this.authHeader = `Basic ${credentials}`;
  }

  async sendOtp(phone: string, otp: string): Promise<void> {
    // ── Override de demo ─────────────────────────────────────────────────────
    // En prod, los overrides nunca cambian del default (enabled: true, slow: false),
    // así que este bloque es un no-op en producción.
    const override = getProviderOverride('clicksend');
    if (!override.enabled) {
      addDemoLog('error', '[ClickSend] Deshabilitado manualmente por demo');
      throw new Error('[DEMO] ClickSend deshabilitado manualmente');
    }
    if (override.slow) {
      addDemoLog('warn', '[ClickSend] Simulando lentitud (timeout)');
      // Esperar más que el timeout de axios para provocar el error
      await new Promise(r => setTimeout(r, 10000));
    }

    // ── Llamada real a ClickSend ──────────────────────────────────────────────
    const response = await axios.post(
      this.baseUrl,
      {
        messages: [
          {
            source:  'banco-dhabi',
            to:      phone,
            body:    `Tu código OTP del Banco Dhabi es: ${otp}. Válido 5 minutos. No lo compartas.`,
          },
        ],
      },
      {
        headers: {
          'Authorization': this.authHeader,
          'Content-Type':  'application/json',
        },
        timeout: 8000,
      },
    );

    // ClickSend retorna http_code 200 pero el status del mensaje puede ser distinto de SUCCESS.
    // En cuentas de prueba, puede retornar COUNTRY_NOT_ENABLED aunque procese el request y lo muestre en el dashboard.
    const rawStatus = response.data?.data?.messages?.[0]?.status;
    const messageStatus = rawStatus ? String(rawStatus).toUpperCase() : null;
    const allowedStatuses = ['SUCCESS', 'QUEUED', 'COUNTRY_NOT_ENABLED'];

    if (messageStatus && !allowedStatuses.includes(messageStatus)) {
      throw new Error(`[ClickSend] Envío fallido — status: ${messageStatus}`);
    }

    addDemoLog('ok', `[ClickSend] SMS enviado a ${phone}`);
  }
}
