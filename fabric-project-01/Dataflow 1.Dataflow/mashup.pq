[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "2020 csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"2020 csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "1db9f52d-94fd-4e17-a9c9-caa852b7cf0c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e7714d98-5bc4-4c39-b40e-0030a7b87f04"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2020.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"SO45347", type text}, {"1", Int64.Type}, {"2020-01-01", type date}, {"Clarence Raji", type text}, {"clarence35@adventure-works.com", type text}, {"Road-650 Black, 52", type text}, {"1_1", Int64.Type}, {"699.0982", type number}, {"55.9279", type number}}),
  #"Demoted headers" = Table.DemoteHeaders(#"Changed column type"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Demoted headers", {{"Column1", type text}, {"Column2", Int64.Type}, {"Column3", type date}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column8", type number}, {"Column9", type number}})
in
  #"Changed column type 1";
shared #"2020 csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "1db9f52d-94fd-4e17-a9c9-caa852b7cf0c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c744b31b-3ca0-44f9-97ff-de099ffb8e83"]}[Data],
  TableNavigation = Navigation_2{[Id = "2020 csv", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "1db9f52d-94fd-4e17-a9c9-caa852b7cf0c"]}[Data]{[lakehouseId = "c744b31b-3ca0-44f9-97ff-de099ffb8e83"]}[Data];
