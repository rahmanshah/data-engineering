CREATE TABLE [dbo].[country] (
    [code]           CHAR (3)        NOT NULL,
    [name]           NVARCHAR (255)  NOT NULL,
    [continent]      NVARCHAR (255)  NOT NULL,
    [region]         NVARCHAR (255)  NOT NULL,
    [surfacearea]    FLOAT (53)      NOT NULL,
    [indepyear]      SMALLINT        NULL,
    [population]     INT             NOT NULL,
    [lifeexpectancy] FLOAT (53)      NULL,
    [gnp]            DECIMAL (10, 2) NULL,
    [gnpold]         DECIMAL (10, 2) NULL,
    [localname]      NVARCHAR (255)  NOT NULL,
    [governmentform] NVARCHAR (255)  NOT NULL,
    [headofstate]    NVARCHAR (255)  NULL,
    [capital]        INT             NULL,
    [code2]          CHAR (2)        NOT NULL,
    PRIMARY KEY CLUSTERED ([code] ASC),
    CONSTRAINT [continent_check] CHECK ([continent]='South America' OR [continent]='Antarctica' OR [continent]='Oceania' OR [continent]='Africa' OR [continent]='North America' OR [continent]='Europe' OR [continent]='Asia'),
    CONSTRAINT [FK_country_city] FOREIGN KEY ([capital]) REFERENCES [dbo].[city] ([id])
);


GO

