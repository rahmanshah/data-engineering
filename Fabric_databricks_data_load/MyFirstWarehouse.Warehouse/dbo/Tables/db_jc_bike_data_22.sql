CREATE TABLE [dbo].[db_jc_bike_data_22] (

	[ride_id] varchar(8000) NULL, 
	[member_casual] varchar(8000) NULL, 
	[rideable_type] varchar(8000) NULL, 
	[started_at] datetime2(6) NULL, 
	[ended_at] datetime2(6) NULL, 
	[start_station_id] varchar(8000) NULL, 
	[end_station_id] varchar(8000) NULL, 
	[start_lat] float NULL, 
	[start_lng] float NULL, 
	[end_lat] float NULL, 
	[end_lng] float NULL
);