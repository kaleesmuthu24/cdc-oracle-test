CREATE TABLE IF NOT EXISTS orders_projection (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    status VARCHAR(30),
    total_cents BIGINT,
    version_no BIGINT,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    source_scn NUMERIC(38,0) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    source_ts_ms BIGINT NOT NULL,
    payload JSONB,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW active_orders AS
SELECT order_id, customer_id, status, total_cents, version_no, source_scn, source_partition, source_offset, source_ts_ms
FROM orders_projection
WHERE is_deleted = FALSE;

CREATE INDEX IF NOT EXISTS idx_orders_projection_source_scn
    ON orders_projection(source_scn);
