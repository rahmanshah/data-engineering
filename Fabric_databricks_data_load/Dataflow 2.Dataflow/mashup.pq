[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "aggregated_bike_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ride_id", DestinationColumnName = "ride_id"], [SourceColumnName = "member_casual", DestinationColumnName = "member_casual"], [SourceColumnName = "rideable_type", DestinationColumnName = "rideable_type"], [SourceColumnName = "started_at", DestinationColumnName = "started_at"], [SourceColumnName = "ended_at", DestinationColumnName = "ended_at"], [SourceColumnName = "trip_duration_minutes", DestinationColumnName = "trip_duration_minutes"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared aggregated_bike_data = let
  Source = DatabricksMultiCloud.Catalogs("dbc-ffb2734f-3900.cloud.databricks.com", "/sql/1.0/warehouses/f9889b398405ad31", [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = "enabled", Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "course_project", Kind = "Database"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "citibike", Kind = "Schema"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "aggregated_bike_data", Kind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "jc_bike_data_22_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ride_id", DestinationColumnName = "ride_id"], [SourceColumnName = "member_casual", DestinationColumnName = "member_casual"], [SourceColumnName = "rideable_type", DestinationColumnName = "rideable_type"], [SourceColumnName = "started_at", DestinationColumnName = "started_at"], [SourceColumnName = "ended_at", DestinationColumnName = "ended_at"], [SourceColumnName = "start_station_id", DestinationColumnName = "start_station_id"], [SourceColumnName = "end_station_id", DestinationColumnName = "end_station_id"], [SourceColumnName = "start_lat", DestinationColumnName = "start_lat"], [SourceColumnName = "start_lng", DestinationColumnName = "start_lng"], [SourceColumnName = "end_lat", DestinationColumnName = "end_lat"], [SourceColumnName = "end_lng", DestinationColumnName = "end_lng"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared jc_bike_data_22 = let
  Source = DatabricksMultiCloud.Catalogs("dbc-ffb2734f-3900.cloud.databricks.com", "/sql/1.0/warehouses/f9889b398405ad31", [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = "enabled", Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "course_project", Kind = "Database"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "citibike", Kind = "Schema"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "jc_bike_data_22", Kind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "products_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ProductID", DestinationColumnName = "ProductID"], [SourceColumnName = "ProductName", DestinationColumnName = "ProductName"], [SourceColumnName = "Category", DestinationColumnName = "Category"], [SourceColumnName = "ListPrice", DestinationColumnName = "ListPrice"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared products = let
  Source = DatabricksMultiCloud.Catalogs("dbc-ffb2734f-3900.cloud.databricks.com", "/sql/1.0/warehouses/f9889b398405ad31", [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = "enabled", Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "course_project", Kind = "Database"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "citibike", Kind = "Schema"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "products", Kind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "station_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "station_id", DestinationColumnName = "station_id"], [SourceColumnName = "station_name", DestinationColumnName = "station_name"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared station_data = let
  Source = DatabricksMultiCloud.Catalogs("dbc-ffb2734f-3900.cloud.databricks.com", "/sql/1.0/warehouses/f9889b398405ad31", [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = "enabled", Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "course_project", Kind = "Database"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "citibike", Kind = "Schema"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "station_data", Kind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "trip_duration_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ride_id", DestinationColumnName = "ride_id"], [SourceColumnName = "trip_duration_minutes", DestinationColumnName = "trip_duration_minutes"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared trip_duration = let
  Source = DatabricksMultiCloud.Catalogs("dbc-ffb2734f-3900.cloud.databricks.com", "/sql/1.0/warehouses/f9889b398405ad31", [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = "enabled", Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "course_project", Kind = "Database"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "citibike", Kind = "Schema"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "trip_duration", Kind = "Table"]}[Data]
in
  #"Navigation 3";
shared aggregated_bike_data_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "a190ac2d-3af1-4ba1-a5de-1aab7e0a6c97"]}[Data],
  TableNavigation = Navigation_2{[Item = "aggregated_bike_data", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared jc_bike_data_22_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "a190ac2d-3af1-4ba1-a5de-1aab7e0a6c97"]}[Data],
  TableNavigation = Navigation_2{[Item = "jc_bike_data_22", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared products_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "a190ac2d-3af1-4ba1-a5de-1aab7e0a6c97"]}[Data],
  TableNavigation = Navigation_2{[Item = "products", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared station_data_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "a190ac2d-3af1-4ba1-a5de-1aab7e0a6c97"]}[Data],
  TableNavigation = Navigation_2{[Item = "station_data", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared trip_duration_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "a190ac2d-3af1-4ba1-a5de-1aab7e0a6c97"]}[Data],
  TableNavigation = Navigation_2{[Item = "trip_duration", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
