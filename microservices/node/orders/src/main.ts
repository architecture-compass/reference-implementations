// Orders service (TypeScript): accepts orders, writes them with an OrderPlaced event to
// the transactional outbox, relays the outbox to Kafka, and confirms orders when
// PaymentCaptured arrives. Catalog calls use timeout, retries and a circuit breaker
// with a last-known-price fallback.
import Fastify from "fastify";
import { Kafka, Partitioners, logLevel } from "kafkajs";
import pg from "pg";
import { randomUUID } from "node:crypto";
import { CircuitBreaker, CircuitOpenError } from "./breaker.js";
import { relayOutbox } from "./outbox.js";

const TOPIC_ORDER_PLACED = "orders.order-placed";
const TOPIC_PAYMENT_CAPTURED = "payments.payment-captured";

const db = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 10 });
const kafka = new Kafka({ clientId: "orders", brokers: [process.env.KAFKA_BROKERS!], logLevel: logLevel.WARN });

type Price = { sku: string; price: string; currency: string };
class UnknownSku extends Error {}
class CatalogUnavailable extends Error {}

// Money as integer cents, so no floating-point rounding.
const toCents = (s: string) => {
  const [whole, frac = ""] = s.split(".");
  return BigInt(whole) * 100n + BigInt((frac + "00").slice(0, 2));
};
const fromCents = (c: bigint) => `${c / 100n}.${(c % 100n).toString().padStart(2, "0")}`;

// ---------- Catalog client: timeout + retries + circuit breaker + fallback ----------

class Catalog {
  private breaker = new CircuitBreaker({ failureThreshold: 3, openMs: 10_000 });
  private cache = new Map<string, Price>();
  constructor(private baseUrl: string) {}

  private async fetchPrice(sku: string): Promise<Price> {
    const res = await fetch(`${this.baseUrl}/prices/${encodeURIComponent(sku)}`, {
      signal: AbortSignal.timeout(500),
    });
    if (res.status === 404) throw new UnknownSku(sku);
    if (!res.ok) throw new Error(`catalog returned ${res.status}`);
    return (await res.json()) as Price;
  }

  async price(sku: string): Promise<Price> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 3; attempt++) { // 1 call + 2 retries
      try {
        // An unknown SKU is a valid answer, not a catalog failure.
        const p = await this.breaker.call(() => this.fetchPrice(sku), (e) => e instanceof UnknownSku);
        this.cache.set(sku, p);
        return p;
      } catch (e) {
        if (e instanceof UnknownSku) throw e;
        lastError = e;
        if (e instanceof CircuitOpenError) break; // don't hammer an open breaker
        await new Promise((r) => setTimeout(r, 100));
      }
    }
    const cached = this.cache.get(sku);
    if (cached) {
      console.warn(`catalog unavailable, using last known price for ${sku}: ${lastError}`);
      return cached;
    }
    throw new CatalogUnavailable(String(lastError));
  }
}

const catalog = new Catalog(process.env.CATALOG_URL!);

// ---------- HTTP API ----------

type OrderRow = { id: string; status: string; amount: string; currency: string };
const view = (r: OrderRow) => ({ orderId: r.id, status: r.status, amount: r.amount, currency: r.currency });

async function findByKey(key: string) {
  const { rows } = await db.query<OrderRow>(
    "SELECT id::text, status, amount::text, currency FROM orders WHERE idempotency_key = $1", [key]);
  return rows[0] ? view(rows[0]) : null;
}

const app = Fastify({ logger: { level: "warn" } });

app.post<{ Body: { customerId?: string; sku?: string; quantity?: number } }>("/orders", async (req, reply) => {
  const key = req.headers["idempotency-key"];
  if (typeof key !== "string" || !key) return reply.code(400).send({ error: "Idempotency-Key header is required" });
  const { customerId, sku, quantity } = req.body ?? {};
  if (!customerId || !sku || !Number.isInteger(quantity) || quantity! < 1) {
    return reply.code(400).send({ error: "customerId, sku and a positive quantity are required" });
  }

  const existing = await findByKey(key); // same key seen before
  if (existing) return reply.code(202).send(existing);

  let price: Price;
  try {
    price = await catalog.price(sku);
  } catch (e) {
    if (e instanceof UnknownSku) return reply.code(404).send({ error: "unknown sku" });
    return reply.code(503).send({ error: `catalog unavailable: ${(e as Error).message}` });
  }

  const amount = fromCents(toCents(price.price) * BigInt(quantity!));
  const orderId = randomUUID();
  const event = {
    eventId: randomUUID(),
    eventType: "OrderPlaced",
    orderId,
    customerId,
    amount,
    currency: price.currency,
    occurredAt: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
  };

  // The order and its event commit together, or not at all.
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO orders (id, customer_id, sku, quantity, amount, currency, status, idempotency_key)
       VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7)`,
      [orderId, customerId, sku, quantity, amount, price.currency, key]);
    await client.query(
      `INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
       VALUES ($1, 'Order', $2, 'OrderPlaced', $3, $4)`,
      [randomUUID(), orderId, TOPIC_ORDER_PLACED, JSON.stringify(event)]);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    if ((e as { code?: string }).code === "23505") { // concurrent request with the same key
      return reply.code(202).send(await findByKey(key));
    }
    throw e;
  } finally {
    client.release();
  }
  return reply.code(202).send({ orderId, status: "PENDING", amount, currency: price.currency });
});

app.get<{ Params: { id: string } }>("/orders/:id", async (req, reply) => {
  const { rows } = await db.query<OrderRow>(
    "SELECT id::text, status, amount::text, currency FROM orders WHERE id::text = $1", [req.params.id]);
  return rows[0] ? view(rows[0]) : reply.code(404).send({ error: "order not found" });
});

app.get("/health", async () => ({ status: "ok" }));

// ---------- PaymentCaptured consumer (idempotent) ----------

async function consumePayments() {
  const consumer = kafka.consumer({ groupId: "orders" });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_PAYMENT_CAPTURED, fromBeginning: true });
  await consumer.run({
    // The offset is committed only after this function resolves, so a failure
    // means the message is delivered again.
    eachMessage: async ({ message }) => {
      let evt: { eventId: string; orderId: string };
      try {
        evt = JSON.parse(message.value!.toString());
      } catch {
        console.error("skipping malformed event");
        return;
      }
      const client = await db.connect();
      try {
        await client.query("BEGIN");
        const first = await client.query(
          "INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING", [evt.eventId]);
        if (first.rowCount === 1) {
          await client.query(
            "UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id::text = $1", [evt.orderId]);
        }
        await client.query("COMMIT");
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        client.release();
      }
    },
  });
}

async function main() {
  const producer = kafka.producer({ idempotent: true, createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  relayOutbox(db, producer);
  await consumePayments();
  await app.listen({ host: "0.0.0.0", port: Number(process.env.PORT ?? 8080) });
  console.log("orders service listening");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
