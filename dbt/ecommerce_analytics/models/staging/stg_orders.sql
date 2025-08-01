WITH source AS (
    SELECT * FROM read_json_auto('{{ project_root }}/data/orders.json')
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
    CAST(order_date AS DATE) AS order_date,
    status,
    CAST(amount AS DECIMAL(10,2)) AS amount
FROM cleaned