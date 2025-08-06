CREATE TABLE [dbo].[departments] (
    [id]      INT            NOT NULL,
    [code]    NVARCHAR (4)   NOT NULL,
    [capital] NVARCHAR (100) NOT NULL,
    [region]  NVARCHAR (4)   NOT NULL,
    [name]    NVARCHAR (255) NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    CONSTRAINT [departments_region_fkey] FOREIGN KEY ([region]) REFERENCES [dbo].[regions] ([code]),
    CONSTRAINT [departments_capital_key] UNIQUE NONCLUSTERED ([capital] ASC),
    CONSTRAINT [departments_code_key] UNIQUE NONCLUSTERED ([code] ASC),
    CONSTRAINT [departments_name_key] UNIQUE NONCLUSTERED ([name] ASC)
);


GO

