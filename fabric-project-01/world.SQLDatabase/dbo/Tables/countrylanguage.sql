CREATE TABLE [dbo].[countrylanguage] (
    [countrycode] CHAR (3)       NOT NULL,
    [language]    NVARCHAR (255) NOT NULL,
    [isofficial]  BIT            NOT NULL,
    [percentage]  FLOAT (53)     NOT NULL,
    CONSTRAINT [PK_countrylanguage] PRIMARY KEY CLUSTERED ([countrycode] ASC, [language] ASC),
    CONSTRAINT [FK_countrylanguage_country] FOREIGN KEY ([countrycode]) REFERENCES [dbo].[country] ([code])
);


GO

