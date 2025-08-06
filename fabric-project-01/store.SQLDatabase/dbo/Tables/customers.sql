CREATE TABLE [dbo].[customers] (
    [customerid]           INT           IDENTITY (1, 1) NOT NULL,
    [firstname]            NVARCHAR (50) NOT NULL,
    [lastname]             NVARCHAR (50) NOT NULL,
    [address1]             NVARCHAR (50) NOT NULL,
    [address2]             NVARCHAR (50) NULL,
    [city]                 NVARCHAR (50) NOT NULL,
    [state]                NVARCHAR (50) NULL,
    [zip]                  INT           NULL,
    [country]              NVARCHAR (50) NOT NULL,
    [region]               SMALLINT      NOT NULL,
    [email]                NVARCHAR (50) NULL,
    [phone]                NVARCHAR (50) NULL,
    [creditcardtype]       INT           NOT NULL,
    [creditcard]           NVARCHAR (50) NOT NULL,
    [creditcardexpiration] NVARCHAR (50) NOT NULL,
    [username]             NVARCHAR (50) NOT NULL,
    [password]             NVARCHAR (50) NOT NULL,
    [age]                  SMALLINT      NULL,
    [income]               INT           NULL,
    [gender]               NVARCHAR (1)  NULL,
    PRIMARY KEY CLUSTERED ([customerid] ASC)
);


GO

CREATE UNIQUE NONCLUSTERED INDEX [ix_cust_username]
    ON [dbo].[customers]([username] ASC);


GO

