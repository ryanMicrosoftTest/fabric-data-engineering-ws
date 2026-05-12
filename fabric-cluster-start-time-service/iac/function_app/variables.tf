variable "environment" {
  description = "Deployment environment (dev, qa, or prod)."
  type        = string

  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "environment must be one of: dev, qa, prod."
  }
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "westus3"
}

variable "fabric_tenant_id" {
  description = "Microsoft Entra tenant ID hosting the Fabric workspace."
  type        = string
}

variable "warehouse_sql_endpoint" {
  description = "Fabric Warehouse SQL endpoint (FQDN) used by the Function App."
  type        = string
}

variable "warehouse_database" {
  description = "Fabric Warehouse database name."
  type        = string
}

variable "target_workspace_ids" {
  description = "List of Fabric workspace IDs the Function App will monitor."
  type        = list(string)
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}

locals {
  # Naming: <service>-<env>-<region-prefix>
  name_prefix = "fcsps-${var.environment}-${substr(var.location, 0, 6)}"

  # Storage account names: globally unique, 3-24 chars, lowercase alphanumeric only.
  # Strip dashes from the prefix and append a short hash suffix derived from
  # the subscription-stable tuple (env + location). Truncate to 24.
  storage_base   = lower(replace("${local.name_prefix}sa", "-", ""))
  storage_suffix = substr(sha1("${var.environment}-${var.location}"), 0, 6)
  storage_account_name = substr(
    "${local.storage_base}${local.storage_suffix}",
    0,
    24,
  )

  resource_group_name       = "${local.name_prefix}-rg"
  managed_identity_name     = "${local.name_prefix}-mi"
  log_analytics_name        = "${local.name_prefix}-law"
  application_insights_name = "${local.name_prefix}-appi"
  service_plan_name         = "${local.name_prefix}-asp"
  function_app_name         = "${local.name_prefix}-func"
}
