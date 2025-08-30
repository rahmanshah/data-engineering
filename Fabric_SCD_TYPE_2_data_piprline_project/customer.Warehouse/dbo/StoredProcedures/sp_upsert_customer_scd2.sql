CREATE   PROCEDURE sp_upsert_customer_scd2
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Step 2: Update existing record if values have changed
        WITH latest_customer AS (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) AS rn
                FROM stg_customer
            ) ranked
            WHERE rn = 1
        )
        UPDATE dim_customer
        SET end_date = lc.last_updated,
            is_current = 0
        FROM dim_customer dc
        JOIN latest_customer lc
          ON dc.customer_id = lc.customer_id
         AND dc.is_current = 1
         AND (
             ISNULL(dc.name, '') <> ISNULL(lc.name, '') OR
             ISNULL(dc.address, '') <> ISNULL(lc.address, '') OR
             ISNULL(dc.phone, '') <> ISNULL(lc.phone, '')
         );

        -- Step 3: Insert new version or new customers
        WITH latest_customer AS (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated DESC) AS rn
                FROM stg_customer
            ) ranked
            WHERE rn = 1
        )
        INSERT INTO dim_customer (
            customer_id, name, address, phone,
            start_date, end_date, is_current
        )
        SELECT lc.customer_id, lc.name, lc.address, lc.phone,
               lc.last_updated, NULL, 1
        FROM latest_customer lc
        LEFT JOIN dim_customer dc
          ON lc.customer_id = dc.customer_id AND dc.is_current = 1
        WHERE dc.customer_id IS NULL
           OR ISNULL(dc.name, '') <> ISNULL(lc.name, '')
           OR ISNULL(dc.address, '') <> ISNULL(lc.address, '')
           OR ISNULL(dc.phone, '') <> ISNULL(lc.phone, '');
           
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- Log error information
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;