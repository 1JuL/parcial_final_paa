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
