CREATE TABLE [dbo].[inventory] (
    [prod_id]       INT NOT NULL,
    [quan_in_stock] INT NOT NULL,
    [sales]         INT NOT NULL,
    PRIMARY KEY CLUSTERED ([prod_id] ASC)
);


GO

