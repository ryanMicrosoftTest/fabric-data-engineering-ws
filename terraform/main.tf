############################################
# Consumer workspace
#
# Deliberately NOT given a OneLake data access role. Enabling OneLake security on
# the consumer item would displace the T-SQL permission model this design depends
# on (notes.md §9.4.1). The absence of a `fabric_onelake_data_access_security`
# resource targeting the consumer lakehouse is load-bearing.
############################################

resource "fabric_workspace" "consumer" {
  display_name = var.consumer_workspace_name
  capacity_id  = var.capacity_id
  description  = "Interim cross-tenant sharing: delegated SQL analytics endpoint + curated views."
}

resource "fabric_workspace_role_assignment" "consumer_principal" {
  workspace_id = fabric_workspace.consumer.id

  principal = {
    id   = var.consumer_principal_object_id
    type = var.consumer_principal_type
  }

  role = var.consumer_workspace_role
}

############################################
# Consumer lakehouse
#
# Schema-enabled so the shortcut lands as a schema rather than loose tables,
# which is what makes DENY SELECT ON SCHEMA:: a usable containment boundary.
############################################

resource "fabric_lakehouse" "consumer" {
  display_name = var.consumer_lakehouse_name
  workspace_id = fabric_workspace.consumer.id
  description  = "Consumer lakehouse. Delegated identity endpoint, no OneLake security."

  configuration = {
    enable_schemas = true
  }
}

############################################
# Passthrough shortcut to the producer schema
#
# The provider's `target.onelake` block exposes no connection_id, so it can only
# ever produce a PASSTHROUGH shortcut. That is exactly what this design requires:
# a delegated (SPN-backed) shortcut is never enumerated by a delegated-mode SQL
# endpoint (notes.md §9.3, Result A). The misconfiguration is unrepresentable here.
############################################

resource "fabric_shortcut" "producer_schema" {
  workspace_id = fabric_workspace.consumer.id
  item_id      = fabric_lakehouse.consumer.id

  name = var.consumer_schema_name
  path = "Tables"

  shortcut_conflict_policy = "CreateOrOverwrite"

  target = {
    onelake = {
      workspace_id = var.producer_workspace_id
      item_id      = var.producer_lakehouse_id
      path         = var.producer_schema_path
    }
  }
}

############################################
# Producer-side OneLake role (optional)
#
# In delegated identity mode the endpoint reads OneLake as the consumer lakehouse's
# OWNING identity, not the caller's. So the producer-side grant must name that owner
# — not the researcher. The researcher intentionally holds nothing at the producer;
# that decoupling is the whole point of the design.
############################################

resource "fabric_onelake_data_access_security" "producer_role" {
  count = var.manage_producer_role ? 1 : 0

  workspace_id = var.producer_workspace_id
  item_id      = var.producer_lakehouse_id
  role_name    = var.producer_role_name

  decision_rules = [
    {
      effect = "Permit"

      permission = [
        {
          attribute_name              = "Path"
          attribute_value_included_in = ["/${var.producer_schema_path}"]
        },
        {
          attribute_name              = "Action"
          attribute_value_included_in = ["Read"]
        }
      ]

      constraints = {
        rows = [
          for v in var.views : {
            table_path = "/${var.producer_schema_path}/${v.source_table}"
            value      = var.producer_row_filter
          }
        ]
      }
    }
  ]

  members = {
    microsoft_entra_members = [
      {
        object_id   = var.producer_role_object_id
        object_type = var.producer_role_object_type
        tenant_id   = var.tenant_id
      }
    ]
  }

  lifecycle {
    precondition {
      condition     = var.producer_role_object_id != null
      error_message = "producer_role_object_id must be set when manage_producer_role = true. It must be the identity that owns the consumer lakehouse, not the consuming researcher."
    }
  }
}
