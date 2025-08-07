CREATE TABLE [dbo].[orderlines] (
    [orderlineid] INT      NOT NULL,
    [orderid]     INT      NOT NULL,
    [prod_id]     INT      NOT NULL,
    [quantity]    SMALLINT NOT NULL,
    [orderdate]   DATE     NOT NULL,
    CONSTRAINT [PK_orderlines] PRIMARY KEY CLUSTERED ([orderid] ASC, [orderlineid] ASC),
    CONSTRAINT [fk_orderid] FOREIGN KEY ([orderid]) REFERENCES [dbo].[orders] ([orderid]) ON DELETE CASCADE
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [ix_orderlines_orderid]
    ON [dbo].[orderlines]([orderid] ASC, [orderlineid] ASC);


GO

