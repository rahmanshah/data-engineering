CREATE   PROCEDURE sp_upsert_dim_customer_scd1
AS
BEGIN
    SET NOCOUNT ON;

    -- Update existing records with newer data
    UPDATE tgt
    SET 
        name = src.name,
        email = src.email,
        address = src.address,
        last_updated_date = src.file_date
    FROM dim_customer tgt
    INNER JOIN stg_customer src
        ON tgt.customer_id = src.customer_id
    WHERE TRY_CAST(src.file_date as DATE) > TRY_CAST(tgt.last_updated_date as DATE)
      AND (
            tgt.name <> src.name OR
            tgt.email <> src.email OR
            tgt.address <> src.address
      );

    -- Insert new records that don't exist
    INSERT INTO dim_customer (customer_id, name, email, address, last_updated_date)
    SELECT customer_id, name, email, address, file_date
    FROM stg_customer src
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dim_customer tgt
        WHERE tgt.customer_id = src.customer_id
    );
END;