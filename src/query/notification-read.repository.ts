import mongoose, { Schema } from 'mongoose';

export interface NotificationReadDoc {
  _id:       string;
  phone:     string;
  status:    string;
  provider:  string | null;
  attempts:  number;
  createdAt: Date;
  updatedAt: Date;
}

const schema = new Schema<NotificationReadDoc>(
  {
    _id:      { type: String, required: true },
    phone:    { type: String, required: true, index: true },
    status:   { type: String, required: true },
    provider: { type: String, default: null },
    attempts: { type: Number, default: 0 },
    createdAt: { type: Date },
    updatedAt: { type: Date },
  },
  { _id: false, timestamps: false },
);

export const NotificationReadModel = mongoose.model<NotificationReadDoc>(
  'notifications_read',
  schema,
);
