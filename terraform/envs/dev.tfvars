# Validated environment (notes.md §11, 2026-09-02).
#
# Applied with:
#   terraform plan  -var-file=envs/dev.tfvars
#   terraform apply -var-file=envs/dev.tfvars

tenant_id   = "35acf02c-4b87-4ae6-9221-ff5cafd430b4"
capacity_id = "REPLACE-ME"

############################################
# Producer — referenced, never modified
############################################
producer_workspace_id = "a8cbda3d-903e-4154-97d9-9a91c95abb42" # Ryan Development Workspace
producer_lakehouse_id = "0386880f-c134-41be-923c-00150c5fbafe" # onelake_security_lh_with_schemas
producer_schema_path  = "Tables/health_dbo"

############################################
# Consumer
############################################
consumer_workspace_name = "guest-workspace-test"
consumer_lakehouse_name = "guest_consumer_lh2"

# Shortcut schema, and the target of the DENY. Must not be "dbo".
consumer_schema_name = "health_pt"

consumer_principal_object_id = "f6bb6577-9aa7-4545-844b-e4ee58c139db"
consumer_principal_upn       = "cohort-researcher@MngEnvMCAP372892.onmicrosoft.com"
consumer_principal_type      = "User"

# Viewer is the validated value. Contributor and above may bypass the SQL
# permission model this design depends on.
consumer_workspace_role = "Viewer"

############################################
# Curated views — the entire permitted surface
############################################
views = [
  {
    name         = "v_neurology_doctors"
    source_table = "doctor_table"
    where        = "department = 'Neurology'"
  }
]

############################################
# Producer-side role
#
# Off: the producer pre-existed and was deliberately left untouched.
# If enabling, producer_role_object_id must be the identity that OWNS the
# consumer lakehouse — NOT the consuming principal.
############################################
manage_producer_role = false
# producer_role_object_id   = "ab6dc298-6ad2-449f-944b-e6c1c759e586" # fabric_cohort_identity_spn
# producer_role_object_type = "ServicePrincipal"
# producer_role_name        = "NeurologyReadRole"
# producer_row_filter       = "department = 'Neurology'"

############################################
# Attestations — see README
############################################

# Flip to true ONLY after confirming in the portal that the SQL analytics
# endpoint is in Delegated identity mode. No API exists for this setting.
# In User identity mode ownership chaining is disabled and every GRANT is inert.
endpoint_delegated_mode_confirmed = false

apply_sql = true
