CREATE TABLE [dbo].[products] (
    [prod_id]        INT             IDENTITY (1, 1) NOT NULL,
    [category]       INT             NOT NULL,
    [title]          NVARCHAR (50)   NOT NULL,
    [actor]          NVARCHAR (50)   NOT NULL,
    [price]          DECIMAL (12, 2) NOT NULL,
    [special]        SMALLINT        NULL,
    [common_prod_id] INT             NOT NULL,
    PRIMARY KEY CLUSTERED ([prod_id] ASC),
    CONSTRAINT [fk_prod_category] FOREIGN KEY ([category]) REFERENCES [dbo].[categories] ([category])
);


GO

CREATE NONCLUSTERED INDEX [ix_prod_category]
    ON [dbo].[products]([category] ASC);


GO

CREATE NONCLUSTERED INDEX [ix_prod_special]
    ON [dbo].[products]([special] ASC);


GO

