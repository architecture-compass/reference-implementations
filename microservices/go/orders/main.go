// Orders service (Go): accepts orders, writes them with an OrderPlaced event to the
// transactional outbox, relays the outbox to Kafka, and confirms orders when
// PaymentCaptured arrives. Catalog calls are protected by timeout, retries and a
// circuit breaker with a last-known-price fallback.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/sony/gobreaker"
)

const (
	topicOrderPlaced     = "orders.order-placed"
	topicPaymentCaptured = "payments.payment-captured"
)

type orderRequest struct {
	CustomerID string `json:"customerId"`
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
}

type orderView struct {
	OrderID  string `json:"orderId"`
	Status   string `json:"status"`
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type price struct {
	SKU      string `json:"sku"`
	Price    string `json:"price"`
	Currency string `json:"currency"`
}

var errUnknownSKU = errors.New("unknown sku")

// ---------- Catalog client: timeout + retries + circuit breaker + fallback ----------

type catalog struct {
	baseURL string
	http    *http.Client
	breaker *gobreaker.CircuitBreaker
	mu      sync.RWMutex
	cache   map[string]price
}

func newCatalog(baseURL string) *catalog {
	return &catalog{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 500 * time.Millisecond},
		cache:   map[string]price{},
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "catalog",
			Timeout: 10 * time.Second, // how long the breaker stays open
			ReadyToTrip: func(c gobreaker.Counts) bool {
				return c.ConsecutiveFailures >= 3
			},
			// A 404 is a valid answer, not a failure of the catalog.
			IsSuccessful: func(err error) bool { return err == nil || errors.Is(err, errUnknownSKU) },
		}),
	}
}

func (c *catalog) fetch(ctx context.Context, sku string) (price, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/prices/"+sku, nil)
	resp, err := c.http.Do(req)
	if err != nil {
		return price{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return price{}, errUnknownSKU
	}
	if resp.StatusCode != http.StatusOK {
		return price{}, fmt.Errorf("catalog returned %d", resp.StatusCode)
	}
	var p price
	return p, json.NewDecoder(resp.Body).Decode(&p)
}

func (c *catalog) Price(ctx context.Context, sku string) (price, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ { // 1 call + 2 retries
		res, err := c.breaker.Execute(func() (interface{}, error) { return c.fetch(ctx, sku) })
		if err == nil {
			p := res.(price)
			c.mu.Lock()
			c.cache[sku] = p
			c.mu.Unlock()
			return p, nil
		}
		if errors.Is(err, errUnknownSKU) {
			return price{}, err
		}
		lastErr = err
		if errors.Is(err, gobreaker.ErrOpenState) {
			break // don't hammer an open breaker
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if p, ok := c.cache[sku]; ok {
		slog.Warn("catalog unavailable, using last known price", "sku", sku, "error", lastErr)
		return p, nil
	}
	return price{}, fmt.Errorf("catalog unavailable: %w", lastErr)
}

// ---------- HTTP API ----------

type server struct {
	db      *pgxpool.Pool
	catalog *catalog
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) findByKey(ctx context.Context, key string) (*orderView, error) {
	var o orderView
	var amount decimal.Decimal
	err := s.db.QueryRow(ctx,
		`SELECT id::text, status, amount, currency FROM orders WHERE idempotency_key = $1`, key).
		Scan(&o.OrderID, &o.Status, &amount, &o.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	o.Amount = amount.StringFixed(2)
	return &o, err
}

func (s *server) placeOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header is required"})
		return
	}
	var req orderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CustomerID == "" || req.SKU == "" || req.Quantity < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "customerId, sku and a positive quantity are required"})
		return
	}
	ctx := r.Context()

	// Same key seen before: return the original order.
	if existing, err := s.findByKey(ctx, key); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	} else if existing != nil {
		writeJSON(w, http.StatusAccepted, existing)
		return
	}

	p, err := s.catalog.Price(ctx, req.SKU)
	if errors.Is(err, errUnknownSKU) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown sku"})
		return
	} else if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	unit, err := decimal.NewFromString(p.Price)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "invalid price from catalog"})
		return
	}
	amount := unit.Mul(decimal.NewFromInt(int64(req.Quantity))).StringFixed(2)

	orderID := uuid.New()
	event, _ := json.Marshal(map[string]string{
		"eventId":    uuid.NewString(),
		"eventType":  "OrderPlaced",
		"orderId":    orderID.String(),
		"customerId": req.CustomerID,
		"amount":     amount,
		"currency":   p.Currency,
		"occurredAt": time.Now().UTC().Format(time.RFC3339),
	})

	// The order and its event commit together, or not at all.
	err = pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO orders (id, customer_id, sku, quantity, amount, currency, status, idempotency_key)
			 VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7)`,
			orderID, req.CustomerID, req.SKU, req.Quantity, amount, p.Currency, key); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload)
			 VALUES ($1, 'Order', $2, 'OrderPlaced', $3, $4)`,
			uuid.New(), orderID.String(), topicOrderPlaced, event)
		return err
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // concurrent request with the same key
		existing, _ := s.findByKey(ctx, key)
		writeJSON(w, http.StatusAccepted, existing)
		return
	} else if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, orderView{OrderID: orderID.String(), Status: "PENDING", Amount: amount, Currency: p.Currency})
}

func (s *server) getOrder(w http.ResponseWriter, r *http.Request) {
	var o orderView
	var amount decimal.Decimal
	err := s.db.QueryRow(r.Context(),
		`SELECT id::text, status, amount, currency FROM orders WHERE id::text = $1`, r.PathValue("id")).
		Scan(&o.OrderID, &o.Status, &amount, &o.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "order not found"})
		return
	} else if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	o.Amount = amount.StringFixed(2)
	writeJSON(w, http.StatusOK, o)
}

// ---------- Outbox relay ----------

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
				return err // rows stay unpublished and are retried
			}
			_, err = tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
			return err
		})
		if err != nil && ctx.Err() == nil {
			slog.Error("outbox relay failed", "error", err)
		}
	}
}

// ---------- PaymentCaptured consumer (idempotent) ----------

func consumePayments(ctx context.Context, db *pgxpool.Pool, r *kafka.Reader) {
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
		var evt struct {
			EventID string `json:"eventId"`
			OrderID string `json:"orderId"`
		}
		if err := json.Unmarshal(m.Value, &evt); err != nil {
			slog.Error("skipping malformed event", "error", err)
			_ = r.CommitMessages(ctx, m)
			continue
		}
		err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx,
				`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, evt.EventID)
			if err != nil || tag.RowsAffected() == 0 {
				return err // duplicate delivery: already handled
			}
			_, err = tx.Exec(ctx,
				`UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id::text = $1`, evt.OrderID)
			return err
		})
		if err != nil {
			slog.Error("processing failed, will retry", "error", err)
			time.Sleep(time.Second)
			continue // not committed, so the message is redelivered
		}
		_ = r.CommitMessages(ctx, m)
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
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: "orders", Topic: topicPaymentCaptured})
	defer reader.Close()

	go relayOutbox(ctx, db, writer)
	go consumePayments(ctx, db, reader)

	s := &server{db: db, catalog: newCatalog(os.Getenv("CATALOG_URL"))}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.placeOrder)
	mux.HandleFunc("GET /orders/{id}", s.getOrder)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	srv := &http.Server{Addr: ":" + os.Getenv("PORT"), Handler: mux}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	slog.Info("orders service listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server", "error", err)
		os.Exit(1)
	}
}
