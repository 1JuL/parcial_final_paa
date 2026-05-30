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
    id:        doc._id as string,
    phone:     doc.phone,
    status:    doc.status as NotificationReadType['status'],
    provider:  doc.provider as NotificationReadType['provider'],
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
    id:        doc._id as string,
    phone:     doc.phone,
    status:    doc.status as NotificationReadType['status'],
    provider:  doc.provider as NotificationReadType['provider'],
    attempts:  doc.attempts,
    createdAt: doc.createdAt,
    updatedAt: doc.updatedAt,
  }));
}
