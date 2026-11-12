// Outbox relay: every 500 ms, lock unpublished rows, send them to Kafka, mark them
// published. If Kafka fails, the transaction rolls back and the rows are retried.
import type { Producer } from "kafkajs";
import type pg from "pg";

export function relayOutbox(db: pg.Pool, producer: Producer): void {
  const tick = async () => {
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      const { rows } = await client.query<{ id: string; topic: string; aggregate_id: string; payload: string }>(
        `SELECT id, topic, aggregate_id, payload::text AS payload FROM outbox
         WHERE published_at IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED`);
      if (rows.length > 0) {
        for (const r of rows) {
          await producer.send({ topic: r.topic, acks: -1, messages: [{ key: r.aggregate_id, value: r.payload }] });
        }
        await client.query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)", [rows.map((r) => r.id)]);
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK").catch(() => {});
      console.error("outbox relay failed; rows stay unpublished and are retried", e);
    } finally {
      client.release();
      setTimeout(tick, 500);
    }
  };
  setTimeout(tick, 500);
}
