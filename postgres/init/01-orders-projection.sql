CREATE TABLE IF NOT EXISTS orders_projection (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status TEXT NOT NULL,
  total_cents BIGINT NOT NULL,
  version_no BIGINT,
  updated_at BIGINT
);
