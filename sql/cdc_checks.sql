

SELECT
    customer_id,
    COUNT(*) AS record_count
FROM dev_shop.dev_tiiina_aminian_silver.customers_current
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT
    customer_id,
    COUNT(*) AS active_versions
FROM dev_shop.dev_tiiina_aminian_silver.customers_history
WHERE __END_AT IS NULL
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT *
FROM dev_shop.dev_tiiina_aminian_silver.customers_history
WHERE __END_AT <= __START_AT;