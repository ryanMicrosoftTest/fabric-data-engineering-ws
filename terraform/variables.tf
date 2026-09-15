############################################
# Tenant / capacity
############################################

variable "tenant_id" {
  description = "Microsoft Entra tenant ID hosting both the producer and consumer workspaces."
  type        = string
}

variable "capacity_id" {
  description = "Fabric capacity ID to bind the consumer workspace to."
  type        = string
}

############################################
# Producer (data owner) — pre-existing, referenced only
############################################

variable "producer_workspace_id" {
  description = <<-EOT
    Workspace ID holding the producing lakehouse. Referenced, never modified,
    unless manage_producer_role = true.
  EOT
  type        = string
}

variable "producer_lakehouse_id" {
  description = "Item ID of the producing, schema-enabled, OneLake-security-enabled lakehouse."
  type        = string
}

variable "producer_schema_path" {
  description = <<-EOT
    Path inside the producer lakehouse that the consumer shortcut targets.
    Schema-level path, e.g. "Tables/health_dbo".
  EOT
  type        = string
  default     = "Tables/health_dbo"

  validation {
    condition     = startswith(var.producer_schema_path, "Tables/")
    error_message = "producer_schema_path must be a Tables/ path — this design shortcuts a schema, not Files."
  }
}

############################################
# Consumer (guest) workspace
############################################

variable "consumer_workspace_name" {
  description = "Display name of the consumer workspace to create."
  type        = string
  default     = "guest-workspace-test"
}

variable "consumer_lakehouse_name" {
  description = "Display name of the consumer lakehouse to create."
  type        = string
  default     = "guest_consumer_lh2"
}

variable "consumer_schema_name" {
  description = <<-EOT
    Schema name the passthrough shortcut is surfaced under in the consumer lakehouse.
    This is the schema the DENY is applied to, so it must NOT be "dbo".
  EOT
  type        = string
  default     = "health_pt"

  validation {
    condition     = lower(var.consumer_schema_name) != "dbo"
    error_message = "consumer_schema_name must not be dbo — the DENY on this schema would override the GRANT on the view."
  }
}

############################################
# Consumer principal (the guest / researcher)
############################################

variable "consumer_principal_object_id" {
  description = "Entra object ID of the consuming principal (the researcher / B2B guest)."
  type        = string
}

variable "consumer_principal_upn" {
  description = <<-EOT
    UPN of the consuming principal, used verbatim for CREATE USER ... FROM EXTERNAL PROVIDER.
    For a B2B guest this is the mangled in-tenant UPN, not their home-tenant address.
  EOT
  type        = string
}

variable "consumer_principal_type" {
  description = "Fabric principal type for the workspace role assignment."
  type        = string
  default     = "User"

  validation {
    condition     = contains(["User", "Group", "ServicePrincipal", "ServicePrincipalProfile"], var.consumer_principal_type)
    error_message = "consumer_principal_type must be one of: User, Group, ServicePrincipal, ServicePrincipalProfile."
  }
}

variable "consumer_workspace_role" {
  description = <<-EOT
    Workspace role granted to the consuming principal.

    Viewer is the validated and intended value. Contributor/Member/Admin may bypass
    the SQL permission model and would invalidate the security posture this design
    depends on.
  EOT
  type        = string
  default     = "Viewer"

  validation {
    condition     = contains(["Viewer", "Contributor", "Member", "Admin"], var.consumer_workspace_role)
    error_message = "consumer_workspace_role must be one of: Viewer, Contributor, Member, Admin."
  }
}

############################################
# The curated view — the only thing the consumer may read
############################################

variable "views" {
  description = <<-EOT
    Curated views created in dbo and granted to the consuming principal.

    Each entry is the full permitted slice: the consumer receives GRANT SELECT on
    the view and nothing else. `source_table` is resolved against the shortcut
    schema (consumer_schema_name).
  EOT
  type = list(object({
    name         = string
    source_table = string
    where        = optional(string)
    columns      = optional(string, "*")
  }))

  default = [
    {
      name         = "v_neurology_doctors"
      source_table = "doctor_table"
      where        = "department = 'Neurology'"
    }
  ]

  validation {
    condition     = length(var.views) > 0
    error_message = "At least one view must be defined — the consumer has no other read path."
  }
}

############################################
# Producer-side role (optional)
############################################

variable "manage_producer_role" {
  description = <<-EOT
    When true, Terraform manages a OneLake data access role on the PRODUCER lakehouse
    granting the consumer lakehouse's owning identity a filtered read.

    Defaults to false: in the validated environment the producer role pre-existed and
    the producer was deliberately left untouched. Set true only when standing up a
    green-field producer as well.
  EOT
  type        = bool
  default     = false
}

variable "producer_role_name" {
  description = "Name of the producer-side OneLake data access role."
  type        = string
  default     = "NeurologyReadRole"
}

variable "producer_role_object_id" {
  description = <<-EOT
    Entra object ID of the identity that OWNS the consumer lakehouse — i.e. the identity
    Terraform authenticates as. This is the identity the SQL endpoint impersonates in
    delegated mode, so this is the principal that needs producer-side access.
  EOT
  type        = string
  default     = null
}

variable "producer_role_object_type" {
  description = "Entra object type of producer_role_object_id."
  type        = string
  default     = "ServicePrincipal"
}

variable "producer_row_filter" {
  description = "T-SQL row predicate applied by the producer-side role."
  type        = string
  default     = "department = 'Neurology'"
}

############################################
# Operator attestations — things Terraform cannot enforce
############################################

variable "endpoint_delegated_mode_confirmed" {
  description = <<-EOT
    Attestation that the consumer lakehouse SQL analytics endpoint is, and will remain,
    in DELEGATED IDENTITY mode.

    There is no Fabric Terraform resource or documented public API for this setting,
    so it cannot be managed declaratively. It is also the single most likely cause of
    this design failing: in User identity mode the security sync service disables
    ownership chaining, and every GRANT in sql/ becomes inert.

    Delegated identity is the DEFAULT for a newly created lakehouse, so on a green-field
    deploy this is an attestation that you will not switch it. Set it to true up front,
    then re-verify in the portal after apply (post_deploy_checklist item 1).
  EOT
  type        = bool
  default     = false

  validation {
    condition     = var.endpoint_delegated_mode_confirmed
    error_message = "Set endpoint_delegated_mode_confirmed = true to acknowledge that the SQL analytics endpoint must stay in Delegated identity mode. In User identity mode this deployment produces a broken, silently permissive result."
  }
}

variable "apply_sql" {
  description = <<-EOT
    Whether Terraform should apply the T-SQL layer (users, views, GRANT/DENY) via
    local-exec. Requires PowerShell, the SqlServer module, and an authenticated az CLI
    on the machine running terraform. Set false to emit the SQL and apply it manually.
  EOT
  type        = bool
  default     = true
}
