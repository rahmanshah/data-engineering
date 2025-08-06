CREATE TABLE [dbo].[towns] (
    [id]         INT            NOT NULL,
    [code]       NVARCHAR (10)  NOT NULL,
    [article]    NVARCHAR (MAX) NULL,
    [name]       NVARCHAR (255) NOT NULL,
    [department] NVARCHAR (4)   NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC),
    CONSTRAINT [towns_department_fkey] FOREIGN KEY ([department]) REFERENCES [dbo].[departments] ([code]),
    CONSTRAINT [towns_code_department_key] UNIQUE NONCLUSTERED ([code] ASC, [department] ASC)
);


GO

