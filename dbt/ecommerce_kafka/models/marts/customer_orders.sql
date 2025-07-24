WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_orders AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.country,
        COUNT(o.order_id) AS number_of_orders,
        SUM(o.amount) AS total_amount
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    customer_id,
    first_name,
    last_name,
    country,
    number_of_orders,
    total_amount,
    CASE
        WHEN total_amount > 1000 THEN 'high_value'
        WHEN total_amount > 500 THEN 'medium_value'
        ELSE 'low_value'
    END AS customer_value_segment
FROM customer_orders