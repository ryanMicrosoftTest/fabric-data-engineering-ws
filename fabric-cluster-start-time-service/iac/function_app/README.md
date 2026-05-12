# Fabric Cluster Start-time Service — Function App IaC

Terraform module that provisions the Azure resources hosting the Fabric Cluster
Start-time Performance Function App (Python 3.11 on Linux Flex Consumption).

## Resources provisioned

- Resource Group
- User-assigned Managed Identity (used for Storage + Fabric access)
- Storage Account (Durable Functions task hub, MI-based — `shared_access_key_enabled = false`)
- Log Analytics Workspace (PerGB2018, 30-day retention)
- Application Insights (workspace-based)
- Linux Service Plan (Flex Consumption, `FC1`)
- Linux Function App (Python 3.11)
- Storage role assignments for the MI:
  - Storage Blob Data Owner
  - Storage Queue Data Contributor
  - Storage Table Data Contributor

## Usage

The backend is intentionally empty; supply it from the pipeline:

```powershell
terraform init `
  -backend-config="resource_group_name=<tfstate-rg>" `
  -backend-config="storage_account_name=<tfstate-sa>" `
  -backend-config="container_name=tfstate" `
  -backend-config="key=fcsps/function_app/dev.tfstate"

terraform plan  -var-file=envs/dev.tfvars
terraform apply -var-file=envs/dev.tfvars
```

Swap `dev.tfvars` for `qa.tfvars` or `prod.tfvars` for higher environments
(and update the backend `key` accordingly).

## Post-deploy manual steps

The `azurerm` provider cannot grant Microsoft Fabric data-plane permissions.
After every successful `terraform apply`, take the
`managed_identity_principal_id` output and grant it the following
**out-of-band** (run by separate scripts, not via Terraform):

1. **Fabric workspace Contributor** on each workspace listed in
   `target_workspace_ids`. Use the Fabric REST API:
   `POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/roleAssignments`
   with body `{ "principal": { "id": "<principal_id>", "type": "ServicePrincipal" }, "role": "Contributor" }`.

2. **`db_owner` on the Fabric Warehouse** (`warehouse_database`). Connect to the
   warehouse SQL endpoint as an admin and run:

   ```sql
   CREATE USER [<managed-identity-name>] FROM EXTERNAL PROVIDER;
   ALTER ROLE db_owner ADD MEMBER [<managed-identity-name>];
   ```

   Use the MI display name (`fcsps-<env>-<region>-mi`) as the external principal.

## Outputs

| Output                                   | Description                                            |
| ---------------------------------------- | ------------------------------------------------------ |
| `function_app_name`                      | Linux Function App name                                |
| `function_app_default_hostname`          | Default hostname (`*.azurewebsites.net`)               |
| `resource_group_name`                    | Resource group containing all resources                |
| `managed_identity_principal_id`          | MI object ID — feed into the Fabric grant scripts      |
| `managed_identity_client_id`             | MI client ID                                           |
| `application_insights_connection_string` | App Insights connection string (sensitive)             |
| `storage_account_name`                   | Storage account backing AzureWebJobsStorage            |
