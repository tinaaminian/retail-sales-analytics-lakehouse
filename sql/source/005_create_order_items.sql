CREATE TABLE IF NOT EXISTS source.order_items(
    order_item_id BIGINT primary key, 
    order_id BIGINT NOT NULL REFERENCES source.orders(order_id),
    product_id BIGINT NOT NULL REFERENCES source.products(product_id),
    quantity INT NOT NULL CHECK(quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK(unit_price >= 0),
    discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK(discount_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);