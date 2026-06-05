environment            = "dev"
location               = "westus3"
fabric_tenant_id       = "35acf02c-4b87-4ae6-9221-ff5cafd430b4"
warehouse_sql_endpoint = "ftykynmhjpteverb75ok7vbqwq-ljdabvz5x3jevlc4bifz6llgwi.datawarehouse.fabric.microsoft.com"
warehouse_database     = "fabric_telemetry_wh"
target_workspace_ids   = ["d700465a-be3d-4ad2-ac5c-0a0b9f2d66b2"]

tags = {
  service     = "fabric-cluster-start-time-service"
  environment = "dev"
  owner       = "data-platform"
}
