[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "customer_branch1_2025-07-15 csv_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "address", DestinationColumnName = "address"], [SourceColumnName = "last_updated", DestinationColumnName = "last_updated"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"customer_branch1_2025-07-15 csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd866274-7f58-4cd8-9357-b54edeb14d40"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "4cf8a5c4-80e1-4fe2-adef-6e83ccb62697"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "source"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "customer_branch1_2025-07-15.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Changed column type" = Table.TransformColumnTypes(#"Imported CSV", {{"Column5", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"customer_id", Int64.Type}, {"name", type text}, {"address", type text}, {"phone", Int64.Type}, {"last_updated", type datetime}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "customer_branch1_2025-07-16 csv_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "address", DestinationColumnName = "address"], [SourceColumnName = "last_updated", DestinationColumnName = "last_updated"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"customer_branch1_2025-07-16 csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "bd866274-7f58-4cd8-9357-b54edeb14d40"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "4cf8a5c4-80e1-4fe2-adef-6e83ccb62697"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "source"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "customer_branch1_2025-07-16.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"customer_id", Int64.Type}, {"name", type text}, {"address", type text}, {"phone", Int64.Type}, {"last_updated", type datetime}})
in
  #"Changed column type";
shared #"customer_branch1_2025-07-15 csv_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "bd866274-7f58-4cd8-9357-b54edeb14d40"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "dea745d3-f7de-4c92-926a-157df71fd5d0"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "stg_customer"]}[Data]
in
  TableNavigation;
shared #"customer_branch1_2025-07-16 csv_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "bd866274-7f58-4cd8-9357-b54edeb14d40"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "dea745d3-f7de-4c92-926a-157df71fd5d0"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "stg_customer"]}[Data]
in
  TableNavigation;
