CREATE TABLE [dbo].[city] (
    [id]          INT            NOT NULL,
    [name]        NVARCHAR (255) NOT NULL,
    [countrycode] CHAR (3)       NOT NULL,
    [district]    NVARCHAR (255) NOT NULL,
    [population]  INT            NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    CONSTRAINT [FK_city_country] FOREIGN KEY ([countrycode]) REFERENCES [dbo].[country] ([code])
);


GO

