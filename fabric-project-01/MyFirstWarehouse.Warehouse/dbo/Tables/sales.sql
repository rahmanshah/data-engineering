CREATE TABLE [dbo].[sales] (

	[SalesOrderNumber] varchar(8000) NULL, 
	[SalesOrderLineNumber] int NULL, 
	[OrderDate] date NULL, 
	[CustomerName] varchar(8000) NULL, 
	[EmailAddress] varchar(8000) NULL, 
	[Item] varchar(8000) NULL, 
	[Quantity] int NULL, 
	[UnitPrice] float NULL, 
	[TaxAmount] float NULL
);