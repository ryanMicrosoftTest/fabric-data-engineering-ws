# OneLake Security Role Pipeline — Setup Guide

## Pipeline Overview

```
┌─────────────────┐     on success     ┌─────────────────┐
│  Role Creation   │ ────────────────→ │  User Mapping    │
│  Notebook        │                    │  Notebook        │
└─────────────────┘                    └─────────────────┘
       ↓                                      ↓
  Creates/updates                       Assigns users to
  OneLake roles                         those roles
       ↓                                      ↓
  Writes to                             Writes to
  role_creation_audit_tbl               role_mapping_audit_tbl
```

- **Role Creation runs first** — creates/updates/deletes roles
- **User Mapping runs second** — only if Role Creation succeeds (roles must exist before members can be assigned)
- Both write audit records to the Fabric SQL Database
- If either notebook fails, the pipeline fails and can alert

## Pipeline Parameters

| Parameter | Description | Example |
|---|---|---|
| `target_workspace_id` | Workspace containing the lakehouse to secure | `a8cbda3d-903e-4154-97d9-9a91c95abb42` |
| `target_lakehouse_id` | Lakehouse to create roles on | `4f8ce569-6f36-4069-81a8-b1c2187b5494` |
| `tenant_id` | Entra tenant ID for member assignment | `35acf02c-4b87-4ae6-9221-ff5cafd430b4` |
| `yaml_role_definitions_path` | ABFSS path to role-definition YAMLs | `abfss://<ws_id>@onelake.dfs.fabric.microsoft.com/<lh_id>/Files/role-definitions` |
| `yaml_role_mappings_path` | ABFSS path to role-mapping YAMLs | `abfss://<ws_id>@onelake.dfs.fabric.microsoft.com/<lh_id>/Files/role-mappings` |
| `kv_uri` | Azure Key Vault URI | `https://kvfabricprodeus2rh.vault.azure.net/` |
| `client_id_secret` | AKV secret name for SPN client ID | `fuam-spn-client-id` |
| `tenant_id_secret` | AKV secret name for SPN tenant ID | `fuam-spn-tenant-id` |
| `client_secret_name` | AKV secret name for SPN client secret | `fuam-spn-secret` |
| `validate_first` | Run dry-run before applying roles | `true` |
| `audit_sql_server` | Fabric SQL Database server | `xyz.database.fabric.microsoft.com` |
| `audit_sql_database` | Fabric SQL Database name | `governance-audit-db` |

## Setup Steps

### 1. Create the Pipeline in Fabric

**Option A: Import from JSON**
1. Open your governance workspace in Fabric
2. Create a new Data Pipeline
3. Switch to the JSON editor (code view)
4. Paste the contents of `pipeline/onelake_security_role_pl.json`
5. Replace the placeholder values:
   - `REPLACE_WITH_ROLE_CREATION_NOTEBOOK_ID` → your notebook's GUID
   - `REPLACE_WITH_USER_MAPPING_NOTEBOOK_ID` → your notebook's GUID
   - `REPLACE_WITH_GOVERNANCE_WORKSPACE_ID` → workspace GUID where notebooks live

**Option B: Build in the Portal**
1. Create a new Data Pipeline
2. Add a **Notebook** activity named "Role Creation"
   - Select the `onelake_role_creation_nb` notebook
   - Add all parameters from the table above
3. Add a second **Notebook** activity named "User Mapping"
   - Set dependency: "Role Creation" → Succeeded
   - Select the `onelake_role_mapping_nb` notebook
   - Add all parameters from the table above

### 2. Set Default Parameter Values

For a per-environment pipeline, set the defaults to match the target environment:

**Dev pipeline defaults:**
```
target_workspace_id = "<dev-data-workspace-id>"
target_lakehouse_id = "<dev-lakehouse-id>"
yaml_role_definitions_path = "abfss://<governance-dev-ws-id>@onelake.dfs.fabric.microsoft.com/<governance-lh-id>/Files/role-definitions"
yaml_role_mappings_path = "abfss://<governance-dev-ws-id>@onelake.dfs.fabric.microsoft.com/<governance-lh-id>/Files/role-mappings"
```

### 3. Configure Schedule

1. Pipeline → Schedule tab
2. Set a daily schedule (e.g., 6:00 AM UTC)
3. The pipeline is idempotent — safe to run multiple times

### 4. Configure Alerts (Optional)

1. Pipeline → Alerts
2. Add alert on "Pipeline Failed"
3. Notify the security team via email or Teams

## Per-Environment Setup

Create one pipeline instance per environment, each with different default parameters:

| Pipeline | Governance Workspace | Target Data Workspace |
|---|---|---|
| `onelake_security_role_pl_dev` | `governance-dev-ws` | `data-dev-ws` |
| `onelake_security_role_pl_test` | `governance-test-ws` | `data-test-ws` |
| `onelake_security_role_pl_prod` | `governance-prod-ws` | `data-prod-ws` |

Same notebooks, same library — only the parameters change.
