[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "db_aggregated_bike_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ride_id", DestinationColumnName = "ride_id"], [SourceColumnName = "member_casual", DestinationColumnName = "member_casual"], [SourceColumnName = "rideable_type", DestinationColumnName = "rideable_type"], [SourceColumnName = "started_at", DestinationColumnName = "started_at"], [SourceColumnName = "ended_at", DestinationColumnName = "ended_at"], [SourceColumnName = "trip_duration_minutes", DestinationColumnName = "trip_duration_minutes"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared db_aggregated_bike_data = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "a190ac2d-3af1-4ba1-a5de-1aab7e0a6c97"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "db_aggregated_bike_data"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each ([started_at] = #datetime(2022, 8, 1, 8, 47, 0) or [started_at] = #datetime(2022, 8, 1, 17, 38, 0)) and ([rideable_type] = "docked_bike"))
in
  #"Filtered rows";
shared db_aggregated_bike_data_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "a190ac2d-3af1-4ba1-a5de-1aab7e0a6c97"]}[Data],
  TableNavigation = Navigation_2{[Item = "db_aggregated_bike_data_docked_casual", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
