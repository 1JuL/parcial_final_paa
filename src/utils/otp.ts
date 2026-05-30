import { randomInt } from 'crypto';

// randomInt es criptográficamente seguro
export function generateOtp(): string {
  return String(randomInt(100000, 999999));
}
