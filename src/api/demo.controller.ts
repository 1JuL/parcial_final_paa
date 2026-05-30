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
      clicksend: store.clicksend,
      twilio:    store.twilio,
    },
  });
});

/**
 * POST /demo/provider
 * Body: { "provider": "clicksend" | "twilio", "enabled": boolean, "slow": boolean }
 * Activa o desactiva un proveedor para la demo
 */
demoRouter.post('/provider', (req: Request, res: Response) => {
  const { provider, enabled, slow } = req.body as {
    provider: 'clicksend' | 'twilio';
    enabled?: boolean;
    slow?:    boolean;
  };

  if (!['clicksend', 'twilio'].includes(provider)) {
    return res.status(400).json({ error: 'provider debe ser "clicksend" o "twilio"' }) as unknown as void;
  }

  const update: { enabled?: boolean; slow?: boolean } = {};
  if (enabled !== undefined) update.enabled = enabled;
  if (slow    !== undefined) update.slow    = slow;

  setProviderOverride(provider, update);
  addDemoLog(
    enabled === false ? 'error' : 'info',
    `[Demo] Proveedor ${provider} → enabled=${enabled ?? '–'}, slow=${slow ?? '–'}`,
  );

  return res.json({ provider, ...update }) as unknown as void;
});

/**
 * POST /demo/cb/force
 * Body: { "state": "CLOSED" | "OPEN" | "HALF_OPEN" }
 * Fuerza el estado del Circuit Breaker
 */
demoRouter.post('/cb/force', (req: Request, res: Response) => {
  const { state } = req.body as { state: CBState };
  if (!['CLOSED', 'OPEN', 'HALF_OPEN'].includes(state)) {
    return res.status(400).json({ error: 'state debe ser CLOSED, OPEN o HALF_OPEN' }) as unknown as void;
  }
  circuitBreaker.forceState(state);
  addDemoLog(
    state === 'OPEN' ? 'error' : state === 'HALF_OPEN' ? 'warn' : 'ok',
    `[Demo] Circuit Breaker forzado a ${state}`,
  );
  return res.json({ state }) as unknown as void;
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
