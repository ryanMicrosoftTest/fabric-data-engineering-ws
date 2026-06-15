environment            = "prod"
location               = "westus3"
fabric_tenant_id       = "REPLACE-ME"
warehouse_sql_endpoint = "REPLACE-ME.datawarehouse.fabric.microsoft.com"
warehouse_database     = "fabric_telemetry_wh"
target_workspace_ids   = ["REPLACE-ME"]

tags = {
  service     = "fabric-cluster-start-time-service"
  environment = "prod"
  owner       = "data-platform"
}
