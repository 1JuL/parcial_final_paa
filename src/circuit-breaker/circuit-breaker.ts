import { env } from '../config/env';

export type CBState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export class CircuitBreaker {
  private state: CBState = 'CLOSED';
  private failureCount: number = 0;
  private lastFailureTime: number = 0;
  
  // Contadores para el estado HALF_OPEN
  private halfOpenSuccessCount: number = 0;
  private readonly halfOpenSuccessThreshold: number = 3; // Éxitos necesarios para volver a CLOSED
  private readonly halfOpenTrafficRatio: number = 0.33;  // % de tráfico que va al primario (33%)

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
        this.halfOpenSuccessCount = 0;
        console.log('[CB] Estado → HALF_OPEN. Iniciando pruebas parciales.');
      } else {
        const result = await fallback();
        return { result, usedProvider: 'FALLBACK' };
      }
    }

    if (this.state === 'HALF_OPEN') {
      // En HALF_OPEN enviamos solo una fracción del tráfico al primario
      if (Math.random() > this.halfOpenTrafficRatio) {
        // Redirigir directamente al fallback sin arriesgar y sin afectar contadores
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
    if (this.state === 'HALF_OPEN') {
      this.halfOpenSuccessCount++;
      console.log(`[CB] Éxito en HALF_OPEN (${this.halfOpenSuccessCount}/${this.halfOpenSuccessThreshold})`);
      if (this.halfOpenSuccessCount >= this.halfOpenSuccessThreshold) {
        this.state = 'CLOSED';
        this.failureCount = 0;
        this.halfOpenSuccessCount = 0;
        console.log('[CB] Estado → CLOSED. Sistema recuperado por completo.');
      }
    } else if (this.state === 'CLOSED') {
      this.failureCount = 0;
    }
  }

  private onFailure(): void {
    if (this.state === 'HALF_OPEN') {
      // Si falla una vez en HALF_OPEN, se vuelve a abrir inmediatamente
      this.state = 'OPEN';
      this.lastFailureTime = Date.now();
      this.halfOpenSuccessCount = 0;
      console.error(`[CB] Estado → OPEN. Falló prueba en HALF_OPEN.`);
    } else {
      this.failureCount++;
      this.lastFailureTime = Date.now();
      if (this.failureCount >= this.failureThreshold) {
        this.state = 'OPEN';
        console.error(`[CB] Estado → OPEN tras ${this.failureCount} fallos.`);
      }
    }
  }

  // Método para el panel de demo — permite forzar el estado manualmente
  forceState(newState: CBState): void {
    const prev = this.state;
    this.state = newState;
    if (newState === 'CLOSED') {
      this.failureCount = 0;
      this.halfOpenSuccessCount = 0;
    } else if (newState === 'HALF_OPEN') {
      this.halfOpenSuccessCount = 0;
    } else if (newState === 'OPEN') {
      this.lastFailureTime = Date.now();
    }
    console.log(`[CB][DEMO] Estado forzado: ${prev} → ${newState}`);
  }

  resetFailures(): void {
    this.failureCount = 0;
    this.halfOpenSuccessCount = 0;
    console.log('[CB][DEMO] Contador de fallos reseteado a 0');
  }

  getState(): CBState       { return this.state; }
  getFailureCount(): number { return this.failureCount; }
  getThreshold(): number    { return this.failureThreshold; }
}

// Singleton: toda la app usa esta instancia
export const circuitBreaker = new CircuitBreaker();
