CREATE TABLE [dbo].[cust_hist] (
    [customerid] INT NOT NULL,
    [orderid]    INT NOT NULL,
    [prod_id]    INT NOT NULL,
    CONSTRAINT [fk_cust_hist_customerid] FOREIGN KEY ([customerid]) REFERENCES [dbo].[customers] ([customerid]) ON DELETE CASCADE
);


GO

CREATE NONCLUSTERED INDEX [ix_cust_hist_customerid]
    ON [dbo].[cust_hist]([customerid] ASC);


GO

