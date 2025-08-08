CREATE TABLE [dbo].[employees] (

	[emp_no] int NOT NULL, 
	[birth_date] date NOT NULL, 
	[first_name] varchar(14) NOT NULL, 
	[last_name] varchar(16) NOT NULL, 
	[gender] char(1) NOT NULL, 
	[hire_date] date NOT NULL
);