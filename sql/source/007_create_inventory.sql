CREATE TABLE IF NOT EXISTS source.inventory(
    inventory_id BIGINT primary key,
    product_id BIGINT NOT NULL REFERENCES source.products(product_id),
    warehouse_code VARCHAR(30) NOT NULL,
    stock_quantity INT NOT NULL CHECK(stock_quantity >= 0),
    reorder_level INT NOT NULL DEFAULT 10 CHECK(reorder_level > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);