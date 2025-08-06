CREATE TABLE [dbo].[reorder] (
    [prod_id]        INT  NOT NULL,
    [date_low]       DATE NOT NULL,
    [quan_low]       INT  NOT NULL,
    [date_reordered] DATE NULL,
    [quan_reordered] INT  NULL,
    [date_expected]  DATE NULL
);


GO

