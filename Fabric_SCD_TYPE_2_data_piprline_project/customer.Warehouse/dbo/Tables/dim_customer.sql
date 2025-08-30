CREATE TABLE [dbo].[dim_customer] (

	[customer_id] varchar(10) NULL, 
	[name] varchar(100) NULL, 
	[address] varchar(200) NULL, 
	[phone] varchar(20) NULL, 
	[start_date] datetime2(0) NULL, 
	[end_date] datetime2(0) NULL, 
	[is_current] varchar(10) NULL
);