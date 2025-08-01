CREATE FUNCTION dbo.get_credit_category (@creditpoints INT)
RETURNS TABLE 
AS 
RETURN 
(
    SELECT 
        CASE 
            WHEN @creditpoints >= 80 THEN 'High'
            WHEN @creditpoints >= 50 THEN 'Average'
            ELSE 'Needs Improvement'
        END AS credit_category
);