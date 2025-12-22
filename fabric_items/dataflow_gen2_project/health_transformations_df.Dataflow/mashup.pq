[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "a8cbda3d-903e-4154-97d9-9a91c95abb42"]}[Data]{[lakehouseId = "a5c23493-8d0b-4355-89cd-f2b150254236"]}[Data];
[BindToDefaultDestination = true]
shared dim_department = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a8cbda3d-903e-4154-97d9-9a91c95abb42"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "5090acaa-9246-4eb2-b9fc-93b8b6e7e60c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_department", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared dim_doctor = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a8cbda3d-903e-4154-97d9-9a91c95abb42"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "5090acaa-9246-4eb2-b9fc-93b8b6e7e60c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_doctor", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared doctor_memos_fact = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a8cbda3d-903e-4154-97d9-9a91c95abb42"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "5090acaa-9246-4eb2-b9fc-93b8b6e7e60c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "doctor_memos_fact", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared Merge = let
  Source = Table.NestedJoin(doctor_memos_fact, {"doctor_id"}, dim_doctor, {"id"}, "dim_doctor", JoinKind.LeftOuter),
  #"Expanded dim_doctor" = Table.ExpandTableColumn(Source, "dim_doctor", {"doctor", "id", "upn"}, {"doctor", "id", "upn"})
in
  #"Expanded dim_doctor";
[BindToDefaultDestination = true]
shared #"Merge (2)" = let
  Source = Table.NestedJoin(Merge, {"doctor"}, dim_department, {"doctor"}, "dim_department", JoinKind.LeftOuter),
  #"Expanded dim_department" = Table.ExpandTableColumn(Source, "dim_department", {"doctor", "department"}, {"doctor.1", "department"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded dim_department", {"doctor_id", "id", "upn", "doctor.1"})
in
  #"Removed columns";
