WITH source AS (
    SELECT * FROM raw_data.customers
),

cleaned AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        country
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    country
FROM cleaned