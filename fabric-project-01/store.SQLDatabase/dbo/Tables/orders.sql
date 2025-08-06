CREATE TABLE [dbo].[orders] (
    [orderid]     INT             IDENTITY (1, 1) NOT NULL,
    [orderdate]   DATE            NOT NULL,
    [customerid]  INT             NULL,
    [netamount]   DECIMAL (12, 2) NOT NULL,
    [tax]         DECIMAL (12, 2) NOT NULL,
    [totalamount] DECIMAL (12, 2) NOT NULL,
    PRIMARY KEY CLUSTERED ([orderid] ASC)
);


GO

CREATE NONCLUSTERED INDEX [ix_order_custid]
    ON [dbo].[orders]([customerid] ASC);


GO

