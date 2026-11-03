CREATE TABLE IF NOT EXISTS source.products(
    product_id BIGINT primary key,
    sku VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL CHECK(unit_price >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);