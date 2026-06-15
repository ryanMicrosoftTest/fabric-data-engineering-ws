############################################
# Resource Group
############################################
resource "azurerm_resource_group" "this" {
  name     = local.resource_group_name
  location = var.location
  tags     = var.tags
}

############################################
# User-assigned Managed Identity
############################################
resource "azurerm_user_assigned_identity" "this" {
  name                = local.managed_identity_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = var.tags
}

############################################
# Storage Account (Durable Functions task hub + AzureWebJobsStorage)
############################################
resource "azurerm_storage_account" "this" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  min_tls_version          = "TLS1_2"

  # Use Managed Identity for all storage access; no shared keys.
  shared_access_key_enabled = false

  # v1: not on a VNet yet. Public network access remains enabled,
  # default action Allow, but we still set AzureServices bypass for clarity.
  public_network_access_enabled = true

  network_rules {
    default_action = "Allow"
    bypass         = ["AzureServices"]
  }

  tags = var.tags
}

############################################
# Log Analytics Workspace
############################################
resource "azurerm_log_analytics_workspace" "this" {
  name                = local.log_analytics_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = var.tags
}

############################################
# Application Insights (workspace-based)
############################################
resource "azurerm_application_insights" "this" {
  name                = local.application_insights_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  workspace_id        = azurerm_log_analytics_workspace.this.id
  application_type    = "web"
  tags                = var.tags
}

############################################
# Linux Service Plan (Flex Consumption)
############################################
resource "azurerm_service_plan" "this" {
  name                = local.service_plan_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = var.tags
}

############################################
# Role assignments on the storage account
############################################
data "azurerm_client_config" "current" {}

# SPN running terraform needs data-plane access to create the deploy container
# (because shared_access_key_enabled = false forces AAD auth).
resource "azurerm_role_assignment" "spn_storage_blob_data_owner" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_role_assignment" "storage_blob_data_owner" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Owner"
  principal_id         = azurerm_user_assigned_identity.this.principal_id
}

resource "azurerm_role_assignment" "storage_queue_data_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_user_assigned_identity.this.principal_id
}

resource "azurerm_role_assignment" "storage_table_data_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Table Data Contributor"
  principal_id         = azurerm_user_assigned_identity.this.principal_id
}

############################################
# Deployment container for Flex Consumption
############################################
resource "azurerm_storage_container" "deploy" {
  name                  = "deploy"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"

  depends_on = [azurerm_role_assignment.spn_storage_blob_data_owner]
}

############################################
# Function App (Flex Consumption, Python 3.13)
############################################
resource "azurerm_function_app_flex_consumption" "this" {
  name                = local.function_app_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  service_plan_id     = azurerm_service_plan.this.id

  storage_container_type            = "blobContainer"
  storage_container_endpoint        = "${azurerm_storage_account.this.primary_blob_endpoint}${azurerm_storage_container.deploy.name}"
  storage_authentication_type       = "UserAssignedIdentity"
  storage_user_assigned_identity_id = azurerm_user_assigned_identity.this.id

  runtime_name    = "python"
  runtime_version = "3.13"

  https_only = true

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.this.id]
  }

  site_config {
    application_insights_connection_string = azurerm_application_insights.this.connection_string
  }

  app_settings = {
    WEBSITE_TIME_ZONE                     = "America/New_York"
    APPLICATIONINSIGHTS_CONNECTION_STRING = azurerm_application_insights.this.connection_string

    # Managed Identity-based AzureWebJobsStorage (no key in app settings).
    AzureWebJobsStorage__accountName = azurerm_storage_account.this.name
    AzureWebJobsStorage__credential  = "managedidentity"
    AzureWebJobsStorage__clientId    = azurerm_user_assigned_identity.this.client_id

    FABRIC_TENANT_ID       = var.fabric_tenant_id
    FABRIC_API_BASE_URL    = "https://api.fabric.microsoft.com"
    WAREHOUSE_SQL_ENDPOINT = var.warehouse_sql_endpoint
    WAREHOUSE_DATABASE     = var.warehouse_database
    MI_CLIENT_ID           = azurerm_user_assigned_identity.this.client_id
    TARGET_WORKSPACE_IDS   = join(",", var.target_workspace_ids)
  }

  tags = var.tags
}

############################################
# TODO: Out-of-band Fabric data-plane grants
#
# The azurerm provider does NOT manage Microsoft Fabric workspace or warehouse
# data-plane permissions. After `terraform apply`, the User-assigned Managed
# Identity (output: managed_identity_principal_id) MUST be granted:
#
#   1. Contributor role on each Fabric workspace listed in
#      var.target_workspace_ids (Fabric REST API: POST
#      /v1/workspaces/{workspaceId}/roleAssignments).
#   2. db_owner (or at minimum read + necessary DML) on the Fabric Warehouse
#      database (var.warehouse_database) via T-SQL:
#          CREATE USER [<mi-name>] FROM EXTERNAL PROVIDER;
#          ALTER ROLE db_owner ADD MEMBER [<mi-name>];
#
# These steps are performed by separate post-deploy scripts; see README.md.
############################################
