WITH source AS (
    SELECT * FROM raw_data.orders
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        amount
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT
    order_id,
    customer_id,
    order_date,
    status,
    amount
FROM cleaned