// Payments service (Go): consumes OrderPlaced, captures each order's payment at most
// once, and publishes PaymentCaptured through its own transactional outbox.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const (
	topicOrderPlaced     = "orders.order-placed"
	topicPaymentCaptured = "payments.payment-captured"
)

type orderPlaced struct {
	EventID string `json:"eventId"`
	OrderID string `json:"orderId"`
	Amount  string `json:"amount"`
}

// handle captures the payment and records the event in one transaction.
// A redelivered event, or a second event for the same order, changes nothing.
func handle(ctx context.Context, db *pgxpool.Pool, evt orderPlaced) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, evt.EventID)
		if err != nil || tag.RowsAffected() == 0 {
			return err // duplicate delivery
		}
		paymentID := uuid.New()
		tag, err = tx.Exec(ctx,
			`INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, 'CAPTURED')
			 ON CONFLICT (order_id) DO NOTHING`, paymentID, evt.OrderID, evt.Amount)
		if err != nil || tag.RowsAffected() == 0 {
			return err // this order was already paid
		}
		// A real service would call the payment provider here, using the
		// order ID as the provider's idempotency key.
		payload, _ := json.Marshal(map[string]string{
			"eventId":    uuid.NewString(),
			"eventType":  "PaymentCaptured",
			"orderId":    evt.OrderID,
			"paymentId":  paymentID.String(),
			"amount":     evt.Amount,
			"occurredAt": time.Now().UTC().Format(time.RFC3339),
		})
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
			 VALUES ($1, 'Payment', $2, 'PaymentCaptured', $3, $4)`,
			uuid.New(), evt.OrderID, topicPaymentCaptured, payload)
		return err
	})
}

func consume(ctx context.Context, db *pgxpool.Pool, r *kafka.Reader) {
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		var evt orderPlaced
		if err := json.Unmarshal(m.Value, &evt); err != nil || evt.EventID == "" {
			slog.Error("skipping malformed event", "error", err)
			_ = r.CommitMessages(ctx, m)
			continue
		}
		if err := handle(ctx, db, evt); err != nil {
			slog.Error("processing failed, will retry", "error", err)
			time.Sleep(time.Second)
			continue
		}
		_ = r.CommitMessages(ctx, m)
	}
}

func relayOutbox(ctx context.Context, db *pgxpool.Pool, w *kafka.Writer) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx,
				`SELECT id, topic, aggregate_id, payload::text FROM outbox
				 WHERE published_at IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED`)
			if err != nil {
				return err
			}
			var ids []uuid.UUID
			var msgs []kafka.Message
			for rows.Next() {
				var id uuid.UUID
				var topic, key, payload string
				if err := rows.Scan(&id, &topic, &key, &payload); err != nil {
					return err
				}
				ids = append(ids, id)
				msgs = append(msgs, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(payload)})
			}
			if len(msgs) == 0 {
				return nil
			}
			if err := w.WriteMessages(ctx, msgs...); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
			return err
		})
		if err != nil && ctx.Err() == nil {
			slog.Error("outbox relay failed", "error", err)
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		slog.Error("database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer writer.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: "payments", Topic: topicOrderPlaced})
	defer reader.Close()

	go relayOutbox(ctx, db, writer)
	go consume(ctx, db, reader)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: ":" + os.Getenv("PORT"), Handler: mux}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	slog.Info("payments service listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server", "error", err)
		os.Exit(1)
	}
}
