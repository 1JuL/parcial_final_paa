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
  if (!phone) return res.status(400).json({ error: 'Campo "phone" requerido' }) as unknown as void;

  try {
    const result = await sendOtpHandler({ phone });
    return res.status(202).json(result) as unknown as void;
  } catch (err) {
    return res.status(500).json({ error: (err as Error).message }) as unknown as void;
  }
});

/**
 * GET /notifications/health/cb
 * Estado del Circuit Breaker
 * IMPORTANTE: debe definirse ANTES de /:id para evitar conflictos de ruta
 */
notificationRouter.get('/health/cb', (_req, res) => {
  res.json({
    state:        circuitBreaker.getState(),
    failureCount: circuitBreaker.getFailureCount(),
    threshold:    circuitBreaker.getThreshold(),
  });
});

/**
 * GET /notifications/phone/:phone
 * Lee desde MongoDB (Query Side)
 */
notificationRouter.get('/phone/:phone', async (req: Request, res: Response) => {
  try {
    const list = await getNotificationsByPhoneHandler(req.params.phone);
    return res.json(list) as unknown as void;
  } catch (err) {
    return res.status(500).json({ error: (err as Error).message }) as unknown as void;
  }
});

/**
 * GET /notifications/:id
 * Lee desde MongoDB (Query Side)
 */
notificationRouter.get('/:id', async (req: Request, res: Response) => {
  try {
    const n = await getNotificationByIdHandler(req.params.id);
    return res.json(n) as unknown as void;
  } catch (err) {
    return res.status(404).json({ error: (err as Error).message }) as unknown as void;
  }
});
