WITH source AS (
    {{ source_query(source('json_files', 'customers')) }}
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
    CAST(created_at AS TIMESTAMP) AS created_at,
    country
FROM cleaned