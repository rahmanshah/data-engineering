CREATE   PROCEDURE sp_upsert_customer_scd2
AS
BEGIN
    -- Step 1: Pick latest record for each customer_id
    WITH latest_customer AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) AS rn
            FROM stg_customer
        ) ranked
        WHERE rn = 1
    )

    -- Step 2: Update existing record if values have changed
    UPDATE dim_customer
    SET end_date = lc.last_updated,
        is_current = 0
    FROM dim_customer dc
    JOIN latest_customer lc
      ON dc.customer_id = lc.customer_id
     AND dc.end_date IS NULL
     AND (
         dc.name <> lc.name OR
         dc.address <> lc.address OR
         dc.phone <> lc.phone
     );

    -- Step 3: Insert new version or new customers
    INSERT INTO dim_customer (
        customer_id, name, address, phone,
        start_date, end_date, is_current
    )
    SELECT lc.customer_id, lc.name, lc.address, lc.phone,
           lc.last_updated, NULL, 1
    FROM latest_customer lc
    LEFT JOIN dim_customer dc
      ON lc.customer_id = dc.customer_id AND dc.end_date IS NULL
    WHERE dc.customer_id IS NULL
       OR dc.name <> lc.name
       OR dc.address <> lc.address
       OR dc.phone <> lc.phone;


END;