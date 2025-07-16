[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "taxi_zone_lookup_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared taxi_zone_lookup = let
  Source = Csv.Document(Web.Contents("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"LocationID", Int64.Type}, {"Borough", type text}, {"Zone", type text}, {"service_zone", type text}})
in
  #"Changed column type";
shared taxi_zone_lookup_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f7bbc500-7787-4138-860d-8bd780c2d7e7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "23928326-44ff-4960-b890-e9dc360f4419"]}[Data],
  TableNavigation = Navigation_2{[Id = "taxi_zone_lookup", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
