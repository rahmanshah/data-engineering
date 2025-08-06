CREATE TABLE [dbo].[regions] (
    [id]      INT            NOT NULL,
    [code]    NVARCHAR (4)   NOT NULL,
    [capital] NVARCHAR (100) NOT NULL,
    [name]    NVARCHAR (255) NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    CONSTRAINT [regions_code_key] UNIQUE NONCLUSTERED ([code] ASC),
    CONSTRAINT [regions_name_key] UNIQUE NONCLUSTERED ([name] ASC)
);


GO

