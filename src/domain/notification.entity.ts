export type NotificationStatus =
  | 'PENDING'        // Guardado en SQL, aún no enviado
  | 'SENT'           // Enviado por ClickSend (proveedor primario)
  | 'FAILED'         // Fallaron ambos proveedores
  | 'FALLBACK_SENT'; // Enviado por Twilio (fallback)

export type SmsProvider = 'CLICKSEND' | 'TWILIO';

// Tal como existe en PostgreSQL (Command Side)
export interface Notification {
  id:           string;
  phone:        string;
  otp:          string;
  status:       NotificationStatus;
  provider:     SmsProvider | null;
  attempts:     number;
  errorMessage: string | null;
  createdAt:    Date;
  updatedAt:    Date;
}

// Tal como existe en MongoDB (Query Side)
// No incluye otp (nunca se expone en lectura)
export interface NotificationReadModel {
  id:        string;
  phone:     string;
  status:    NotificationStatus;
  provider:  SmsProvider | null;
  attempts:  number;
  createdAt: Date;
  updatedAt: Date;
}
