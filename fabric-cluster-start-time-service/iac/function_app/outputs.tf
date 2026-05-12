output "function_app_name" {
  description = "Name of the Linux Function App."
  value       = azurerm_linux_function_app.this.name
}

output "function_app_default_hostname" {
  description = "Default hostname of the Function App."
  value       = azurerm_linux_function_app.this.default_hostname
}

output "resource_group_name" {
  description = "Resource group containing all resources."
  value       = azurerm_resource_group.this.name
}

output "managed_identity_principal_id" {
  description = "Principal (object) ID of the User-assigned Managed Identity. Use this for out-of-band Fabric grants."
  value       = azurerm_user_assigned_identity.this.principal_id
}

output "managed_identity_client_id" {
  description = "Client ID of the User-assigned Managed Identity."
  value       = azurerm_user_assigned_identity.this.client_id
}

output "application_insights_connection_string" {
  description = "Application Insights connection string."
  value       = azurerm_application_insights.this.connection_string
  sensitive   = true
}

output "storage_account_name" {
  description = "Storage account backing the Durable Functions task hub."
  value       = azurerm_storage_account.this.name
}
