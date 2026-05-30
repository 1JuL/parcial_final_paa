import { Kafka, Admin, Producer, Partitioners } from "kafkajs";
import { env } from "./env";
import { KAFKA_TOPICS } from "./../kafka/topics";

export const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers: env.KAFKA_BROKERS.split(","),
  // En producción con Confluent Cloud, agregar: ssl: true y sasl: { ... }
});

// Producer singleton — compartido por toda la app
export const kafkaProducer: Producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

// Admin client — solo para crear topics al arrancar
export async function setupKafkaTopics(): Promise<void> {
  const admin: Admin = kafka.admin();
  await admin.connect();

  await admin.createTopics({
    waitForLeaders: true,
    topics: KAFKA_TOPICS.map((topic) => ({
      topic,
      numPartitions: 3,
      replicationFactor: 1, // Cambiar a 3 en producción
    })),
  });

  await admin.disconnect();
  console.log("[Kafka] Topics creados o verificados:", KAFKA_TOPICS);
}
