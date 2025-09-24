CREATE PROCEDURE dbo.usp_RenameTable
    @OldTableName VARCHAR(128),
    @NewTableName VARCHAR(128)
AS
BEGIN
    -- Renames the table using the system stored procedure 'sp_rename'
    EXEC sp_rename @OldTableName, @NewTableName;
END