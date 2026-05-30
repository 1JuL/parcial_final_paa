import { kafka } from "../../config/kafka";
import { env } from "../../config/env";
import { NotificationReadModel } from "./../../query/notification-read.repository";
import { TOPIC_NOTIFICATION_SENT, TOPIC_NOTIFICATION_FAILED } from "../topics";
import { NotificationSentEvent, NotificationFailedEvent } from "../../domain/notification.events";
import { addDemoLog } from "../../demo/demo.store";

/**
 * Consumer A — Mongo Sync
 * Escucha: notification.sent, notification.failed
 * Acción: Upsert idempotente en MongoDB (Query Side)
 */
export async function startMongoSyncConsumer(): Promise<void> {
  const consumer = kafka.consumer({
    groupId: `${env.KAFKA_GROUP_ID}-mongo-sync`,
  });

  await consumer.connect();
  await consumer.subscribe({
    topics: [TOPIC_NOTIFICATION_SENT, TOPIC_NOTIFICATION_FAILED],
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString()) as
        | NotificationSentEvent
        | NotificationFailedEvent;

      // Upsert idempotente: si el documento ya existe (por reintento de Kafka),
      // simplemente se actualiza con los mismos datos. Sin efectos secundarios.
      await NotificationReadModel.updateOne(
        { _id: event.id },
        {
          $set: {
            phone: event.phone,
            status:
              event.eventType === "notification.sent"
                ? (event as NotificationSentEvent).status
                : "FAILED",
            provider:
              event.eventType === "notification.sent"
                ? (event as NotificationSentEvent).provider
                : null,
            attempts: event.attempts,
            updatedAt: new Date(event.updatedAt),
          },
          $setOnInsert: {
            createdAt: new Date(event.updatedAt),
          },
        },
        { upsert: true },
      );

      addDemoLog("info", `[MongoSync] Upsert: ${event.id} (${topic})`);
    },
  });

  console.log("[Consumer A] Mongo Sync consumer iniciado");
}
