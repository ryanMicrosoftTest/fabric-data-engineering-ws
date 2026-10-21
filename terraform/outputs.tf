output "consumer_workspace_id" {
  description = "ID of the consumer workspace."
  value       = fabric_workspace.consumer.id
}

output "consumer_lakehouse_id" {
  description = "ID of the consumer lakehouse."
  value       = fabric_lakehouse.consumer.id
}

output "sql_endpoint_id" {
  description = "SQL analytics endpoint item ID. Needed for the sqlAudit settings API."
  value       = fabric_lakehouse.consumer.properties.sql_endpoint_properties.id
}

output "sql_endpoint_server" {
  description = "SQL analytics endpoint FQDN. Connect here as the consuming principal to validate."
  value       = fabric_lakehouse.consumer.properties.sql_endpoint_properties.connection_string
}

output "sql_endpoint_provisioning_status" {
  description = "Provisioning status. Anything other than Success means the endpoint is not queryable yet."
  value       = fabric_lakehouse.consumer.properties.sql_endpoint_properties.provisioning_status
}

output "shortcut_schema" {
  description = "Schema the producer data is surfaced under. Denied to the consuming principal."
  value       = var.consumer_schema_name
}

output "granted_views" {
  description = "Fully qualified views the consuming principal may read. This is the complete permitted surface."
  value       = [for v in var.views : "dbo.${v.name}"]
}

output "rendered_sql_path" {
  description = "Directory holding the exact SQL that was applied."
  value       = local.rendered_dir
}

output "post_deploy_checklist" {
  description = "Steps Terraform cannot perform or verify. Read this."
  value = [
    "1. Confirm the SQL analytics endpoint is in DELEGATED IDENTITY mode (Lakehouse > SQL endpoint > Settings). Terraform cannot read or set this. User identity mode disables ownership chaining and silently breaks every grant here.",
    "2. Confirm NO OneLake data access roles exist on ${fabric_lakehouse.consumer.display_name}. Any role enables OneLake security and displaces the SQL permission model.",
    "3. Sign in AS the consuming principal and run .rendered/consumer_probe.sql. Deployer-run verification cannot prove the consumer's experience.",
    "4. Expect the shortcut schema '${var.consumer_schema_name}' to be invisible in Object Explorer for that principal, and the granted views to return the filtered slice.",
  ]
}
