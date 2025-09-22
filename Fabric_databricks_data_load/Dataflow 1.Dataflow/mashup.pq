[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "42024307-0718-493b-91e4-aeb2d0c9d324"]}[Data]{[lakehouseId = "b3974de1-8c5e-47ce-88b2-985516ebdc39"]}[Data];
[BindToDefaultDestination = true]
shared jc_bike_data_22 = let
  Source = DatabricksMultiCloud.Catalogs("dbc-ffb2734f-3900.cloud.databricks.com", "/sql/1.0/warehouses/f9889b398405ad31", [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = "enabled", Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "course_project", Kind = "Database"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "citibike", Kind = "Schema"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "vw_jc_bike_data_22", Kind = "View"]}[Data]
in
  #"Navigation 3";
