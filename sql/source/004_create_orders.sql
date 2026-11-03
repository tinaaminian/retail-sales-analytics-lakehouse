CREATE TABLE IF NOT EXISTS source.orders(
    order_id BIGINT primary key,
    customer_id BIGINT NOT NULL REFERENCES source.customers(customer_id),
    order_date TIMESTAMPTZ NOT NULL,
    status VARCHAR(30) NOT NULL CHECK(status IN ('PENDING', 'PAID', 'DELIVERED', 'CANCLLED')),
    shipping_city VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);