-- The application database: the "source system" the lakehouse copies from.
CREATE TABLE orders (
    order_id       text PRIMARY KEY,
    customer_id    text           NOT NULL,
    customer_email text           NOT NULL,   -- personal data: kept out of silver and gold
    status         text           NOT NULL,   -- PENDING, PAID, SHIPPED, DELIVERED, CANCELLED
    amount         numeric(12, 2) NOT NULL,
    currency       text           NOT NULL,
    created_at     timestamptz    NOT NULL
);

-- Eight orders over the last three days. Times are relative to today, so the demo
-- always lands inside the gold table's recompute window.
INSERT INTO orders VALUES
  ('o-1001', 'c-1', 'ana@example.com',   'DELIVERED', 120.00, 'USD', date_trunc('day', now()) - interval '2 days' + interval '09 hours'),
  ('o-1002', 'c-2', 'ben@example.com',   'SHIPPED',    45.50, 'USD', date_trunc('day', now()) - interval '2 days' + interval '14 hours'),
  ('o-1003', 'c-3', 'chen@example.com',  'CANCELLED',  80.00, 'USD', date_trunc('day', now()) - interval '2 days' + interval '16 hours'),
  ('o-1004', 'c-1', 'ana@example.com',   'PAID',       60.00, 'EUR', date_trunc('day', now()) - interval '1 day'  + interval '10 hours'),
  ('o-1005', 'c-4', 'dev@example.com',   'PAID',      210.00, 'USD', date_trunc('day', now()) - interval '1 day'  + interval '11 hours'),
  ('o-1006', 'c-5', 'eva@example.com',   'PENDING',    99.90, 'USD', date_trunc('day', now()) - interval '1 day'  + interval '18 hours'),
  ('o-1007', 'c-2', 'ben@example.com',   'PAID',       15.25, 'EUR', date_trunc('day', now())                     + interval '01 hours'),
  ('o-1008', 'c-6', 'femi@example.com',  'PENDING',    33.00, 'USD', date_trunc('day', now())                     + interval '02 hours');
