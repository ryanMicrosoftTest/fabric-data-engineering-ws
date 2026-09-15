# Higher environment. Fill in before use.

tenant_id   = "REPLACE-ME"
capacity_id = "REPLACE-ME"

############################################
# Producer — referenced, never modified
############################################
producer_workspace_id = "REPLACE-ME"
producer_lakehouse_id = "REPLACE-ME"
producer_schema_path  = "Tables/REPLACE-ME"

############################################
# Consumer
############################################
consumer_workspace_name = "REPLACE-ME"
consumer_lakehouse_name = "REPLACE-ME"

# Shortcut schema, and the target of the DENY. Must not be "dbo".
consumer_schema_name = "REPLACE-ME"

consumer_principal_object_id = "REPLACE-ME"
consumer_principal_upn       = "REPLACE-ME"
consumer_principal_type      = "User"

# Do not raise this without re-validating. Contributor and above may bypass
# the SQL permission model this design depends on.
consumer_workspace_role = "Viewer"

############################################
# Curated views — the entire permitted surface
############################################
views = [
  {
    name         = "REPLACE-ME"
    source_table = "REPLACE-ME"
    where        = "REPLACE-ME"
  }
]

############################################
# Producer-side role
#
# If enabling, producer_role_object_id must be the identity that OWNS the
# consumer lakehouse — NOT the consuming principal.
############################################
manage_producer_role = false
# producer_role_object_id   = "REPLACE-ME"
# producer_role_object_type = "ServicePrincipal"
# producer_role_name        = "REPLACE-ME"
# producer_row_filter       = "REPLACE-ME"

############################################
# Attestations — see README
############################################

# Flip to true ONLY after confirming in the portal that the SQL analytics
# endpoint is in Delegated identity mode. No API exists for this setting.
# In User identity mode ownership chaining is disabled and every GRANT is inert.
endpoint_delegated_mode_confirmed = false

apply_sql = true
