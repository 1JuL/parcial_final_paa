export interface SmsProvider {
  /**
   * Envía un OTP por SMS.
   * @throws Error si el envío falla (el Circuit Breaker captura esta excepción)
   */
  sendOtp(phone: string, otp: string): Promise<void>;
}
