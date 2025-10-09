az ml datastore create --name <datastore-name> \
  --type azure_data_lake_gen2 \
  --account-name <storage-account-name> \
  --filesystem <container-name> \
  --resource-group <resource-group-name> \
  --workspace-name <workspace-name> \
  --auth-mode <auth-type>



  # MLTable
type: mltable
paths:
  - folder: abfss://<container>@<storage_account>.dfs.core.windows.net/<delta-table-path>
transformations:
  - read_delta_lake:
      # Optional: specify version or timestamp
      # version_as_of: 1
      # timestamp_as_of: '2025-08-26T00:00:00Z'
