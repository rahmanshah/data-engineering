[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "SalesOrderNumber", DestinationColumnName = "SalesOrderNumber"], [SourceColumnName = "SalesOrderLineNumber", DestinationColumnName = "SalesOrderLineNumber"], [SourceColumnName = "OrderDate", DestinationColumnName = "OrderDate"], [SourceColumnName = "CustomerName", DestinationColumnName = "CustomerName"], [SourceColumnName = "EmailAddress", DestinationColumnName = "EmailAddress"], [SourceColumnName = "Item", DestinationColumnName = "Item"], [SourceColumnName = "Quantity", DestinationColumnName = "Quantity"], [SourceColumnName = "UnitPrice", DestinationColumnName = "UnitPrice"], [SourceColumnName = "TaxAmount", DestinationColumnName = "TaxAmount"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared sales = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "1db9f52d-94fd-4e17-a9c9-caa852b7cf0c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e7714d98-5bc4-4c39-b40e-0030a7b87f04"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sales", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared sales_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "1db9f52d-94fd-4e17-a9c9-caa852b7cf0c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "7fa8922e-be68-4074-9e1e-07dce3c59275"]}[Data],
  TableNavigation = Navigation_2{[Item = "sales", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
