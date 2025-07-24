WITH source AS (
    SELECT * FROM {{ ref('orders') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        amount
    FROM source
    WHERE order_date IS NOT NULL
)

SELECT
    order_id,
    customer_id,
    order_date,
    status,
    amount
FROM cleaned