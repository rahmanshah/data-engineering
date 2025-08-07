CREATE TABLE [dbo].[inventory] (
    [prod_id]       INT NOT NULL,
    [quan_in_stock] INT NOT NULL,
    [sales]         INT NOT NULL,
    PRIMARY KEY CLUSTERED ([prod_id] ASC),
    CONSTRAINT [fk_inventory_prod_id] FOREIGN KEY ([prod_id]) REFERENCES [dbo].[products] ([prod_id])
);


GO

