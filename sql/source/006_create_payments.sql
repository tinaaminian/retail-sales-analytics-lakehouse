CREATE TABLE IF NOT EXISTS source.payments(
    payment_id BIGINT primary key,
    order_id BIGINT NOT NULL REFERENCES source.orders(order_id),
    payment_method VARCHAR(30) NOT NULL,
    amount NUMERIC(12,2) NOT NULL CHECK(amount > 0),
    payment_status VARCHAR(30) NOT NULL CHECK (payment_status IN (
    'PENDING',
    'COMPLETED',
    'FAILED',
    'REFUNDED'
)),
    paid_at TIMESTAMPTZ ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);