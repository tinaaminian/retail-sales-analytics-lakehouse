ALTER TABLE source.orders
DROP CONSTRAINT orders_status_check;

ALTER TABLE source.orders
ADD CONSTRAINT orders_status_check
CHECK (
    status IN (
        'PENDING',
        'PAID',
        'SHIPPED',
        'DELIVERED',
        'CANCELLED'
    )
);