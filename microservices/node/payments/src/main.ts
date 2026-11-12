// Payments service (TypeScript): consumes OrderPlaced, captures each order's payment at
// most once, and publishes PaymentCaptured through its own transactional outbox.
import Fastify from "fastify";
import { Kafka, Partitioners, logLevel } from "kafkajs";
import pg from "pg";
import { randomUUID } from "node:crypto";
import { relayOutbox } from "./outbox.js";

const TOPIC_ORDER_PLACED = "orders.order-placed";
const TOPIC_PAYMENT_CAPTURED = "payments.payment-captured";

const db = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
const kafka = new Kafka({ clientId: "payments", brokers: [process.env.KAFKA_BROKERS!], logLevel: logLevel.WARN });

type OrderPlaced = { eventId: string; orderId: string; amount: string };

// Capture the payment and record the event in one transaction.
// A redelivered event, or a second event for the same order, changes nothing.
async function handle(evt: OrderPlaced) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const first = await client.query(
      "INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING", [evt.eventId]);
    if (first.rowCount === 1) {
      const paymentId = randomUUID();
      const captured = await client.query(
        `INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, 'CAPTURED')
         ON CONFLICT (order_id) DO NOTHING`, [paymentId, evt.orderId, evt.amount]);
      if (captured.rowCount === 1) {
        // A real service would call the payment provider here, using the
        // order ID as the provider's idempotency key.
        const event = {
          eventId: randomUUID(),
          eventType: "PaymentCaptured",
          orderId: evt.orderId,
          paymentId,
          amount: evt.amount,
          occurredAt: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
        };
        await client.query(
          `INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
           VALUES ($1, 'Payment', $2, 'PaymentCaptured', $3, $4)`,
          [randomUUID(), evt.orderId, TOPIC_PAYMENT_CAPTURED, JSON.stringify(event)]);
      }
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function main() {
  const producer = kafka.producer({ idempotent: true, createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  relayOutbox(db, producer);

  const consumer = kafka.consumer({ groupId: "payments" });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_ORDER_PLACED, fromBeginning: true });
  await consumer.run({
    // The offset is committed only after this resolves; a failure means redelivery.
    eachMessage: async ({ message }) => {
      let evt: OrderPlaced;
      try {
        evt = JSON.parse(message.value!.toString());
      } catch {
        console.error("skipping malformed event");
        return;
      }
      await handle(evt);
    },
  });

  const app = Fastify({ logger: { level: "warn" } });
  app.get("/health", async () => ({ status: "ok" }));
  await app.listen({ host: "0.0.0.0", port: Number(process.env.PORT ?? 8080) });
  console.log("payments service listening");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
