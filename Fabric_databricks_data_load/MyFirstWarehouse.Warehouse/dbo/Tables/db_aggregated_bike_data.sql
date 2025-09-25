CREATE TABLE [dbo].[db_aggregated_bike_data] (

	[ride_id] varchar(8000) NULL, 
	[member_casual] varchar(8000) NULL, 
	[rideable_type] varchar(8000) NULL, 
	[started_at] datetime2(6) NULL, 
	[ended_at] datetime2(6) NULL, 
	[trip_duration_minutes] bigint NULL
);