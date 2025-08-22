# Fabric CI/CD Deployment Repository

This repository provides a shared, abstracted codebase for Microsoft Fabric CI/CD deployments. Product teams sync their Fabric items into this repo and leverage a common deployment framework without duplicating CI/CD logic.

The framework leverages this project here: https://github.com/microsoft/fabric-cicd

## Purpose
- Centralize CI/CD logic for Fabric deployments.
- Keep team-specific assets (notebooks, lakehouses, etc.) separated from pipeline/framework code.
- Enforce consistent structure and configuration across environments (DEV/QA/PROD).

## Repository Structure
```
.
├─ attached_config/
│  └─ target_deployment.yml   # Required: framework configuration
├─ fabric_items/               # Required folder name
│  ├─ parameter.yml            # Required: environment replacements
│  ├─ <your Fabric items>      # e.g., *.Notebook, *.Lakehouse, etc.
└─ README.md
```

Notes
- The folder name fabric_items is required by the framework. Do not rename it.
- The file parameter.yml must live inside the fabric_items directory.

## How Deployments Work (at a glance)
1. Teams sync Fabric-created items (e.g., Lakehouse, Notebook) into fabric_items/.
2. Repo owners maintain attached_config/target_deployment.yml to define the target workspace and items in scope.
3. Repo owners maintain fabric_items/parameter.yml to apply environment-specific value replacements.
4. On commit to eligible branches, the pipeline runs and deploys items in scope to the configured Fabric workspace.

## target_deployment.yml
This file passes critical information to the framework:
- target_workspace_name: Human-readable Fabric workspace name.
- target_workspace_id: GUID of the target Fabric workspace.
- repo_directory: Must be fabric_items (hard-coded in the framework).
- item_type_in_scope: List of item types to deploy. If Notebook is included, all items with .Notebook under fabric_items will be deployed; similarly for other types.

Example
```yaml
# attached_config/target_deployment.yml
# Names/keys may vary by framework version; align with your framework docs.
target_workspace_name: "Contoso Fabric DEV"
target_workspace_id: "00000000-0000-0000-0000-000000000000"
repo_directory: "fabric_items"   # Do not change
item_type_in_scope:
  - "Notebook"
  - "Lakehouse"
```

## parameter.yml
This file is used to find and replace environment-specific values needed for deployment (e.g., workspace_id, item_id, Key Vault URIs, AAD tenant/client secrets). It must be located at fabric_items/parameter.yml.

For more information, please navigate here: https://microsoft.github.io/fabric-cicd/latest/how_to/parameterization/#parameterization

Example (illustrative only)
```yaml
key_value_replace:
  - find_key: $.variables[?(@.name=="workspace_id")].value
    replace_value:
      DEV: <workspace-guid-dev>
      QA: <workspace-guid-qa>
      PROD: <workspace-guid-prod>
    item_type: "VariableLibrary"
    item_name: "vars"
  - find_key: $.variables[?(@.name=="kv_uri")].value
    replace_value:
      DEV: https://<your-kv-dev>.vault.azure.net/
      QA: https://<your-kv-qa>.vault.azure.net/
      PROD: https://<your-kv-prod>.vault.azure.net/
    item_type: "VariableLibrary"
    item_name: "vars"
# Add additional keys as required by your items and framework
```

Tips
- Keep values for DEV/QA/PROD in sync with your enterprise Key Vault and app registrations.
- Add/adjust entries to match the variables used by your Fabric items.

## Pipeline and Error Logs
- The CI/CD pipeline deploys only the item types listed in item_type_in_scope.
- On failure, an error log is published as a pipeline artifact for debugging (e.g., errorlog artifact containing fabric_cicd.error.log). Check the pipeline run’s Artifacts tab and logs for details.

## Getting Started
1. Create or verify attached_config/target_deployment.yml with the correct workspace info and item types in scope.
2. Place or sync your Fabric items into fabric_items/ (keep the folder name unchanged).
3. Update fabric_items/parameter.yml with the required find/replace mappings for each environment.
4. Commit and push. The pipeline will run and deploy the items in scope to the target workspace.

## Images and Diagrams

- Overall Architecture
  ![Overall Architecture](docs/architecture-overview.png)


## Troubleshooting
- Ensure repo_directory is set to fabric_items.
- Ensure parameter.yml exists at fabric_items/parameter.yml and contains the required keys.
- Confirm item_type_in_scope contains the item types you expect to deploy.
- Verify workspace ID/name values are correct and the service connection has required permissions.
