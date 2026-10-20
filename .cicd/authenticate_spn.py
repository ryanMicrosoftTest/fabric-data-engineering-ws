"""Authenticate via Service Principal and deploy Fabric items.

Usage:
    python authenticate_spn.py <environment> <config_file_path>

The config file should be a YAML file with environment-keyed sections:

    DEV:
      target_workspace_id: <workspace-id>
      repo_directory: fabric_items
      items_in_scope:
        - Notebook
        - DataPipeline

Environment variables required:
    AZURE_TENANT_ID      - Azure AD tenant ID
    AZURE_CLIENT_ID      - Service principal client ID
    AZURE_CLIENT_SECRET  - Service principal client secret
"""

import os
import sys
import time

import yaml
from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace, publish_all_items


def main():
    if len(sys.argv) < 3:
        print("Usage: python authenticate_spn.py <environment> <config_file_path>")
        sys.exit(1)

    environment = sys.argv[1]
    config_file_path = sys.argv[2]

    tenant_id = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        print("ERROR: AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET environment variables must be set.")
        sys.exit(1)

    print(f"Authenticating as service principal for environment: {environment}")
    print(f"Config file: {config_file_path}")

    # Load the config file
    with open(config_file_path, "r") as f:
        config = yaml.safe_load(f)

    if environment not in config:
        print(f"ERROR: Environment '{environment}' not found in config file. Available: {list(config.keys())}")
        sys.exit(1)

    env_config = config[environment]
    workspace_id = env_config.get("target_workspace_id")
    repo_directory = env_config.get("repo_directory", "fabric_items")
    items_in_scope = env_config.get("items_in_scope")

    if not workspace_id:
        print(f"ERROR: 'target_workspace_id' not found for environment '{environment}'.")
        sys.exit(1)

    # Resolve repo_directory relative to the config file
    config_dir = os.path.dirname(os.path.abspath(config_file_path))
    repository_directory = os.path.normpath(os.path.join(config_dir, "..", repo_directory))

    print(f"Workspace ID: {workspace_id}")
    print(f"Repository directory: {repository_directory}")
    print(f"Item types in scope: {items_in_scope}")

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        repository_directory=repository_directory,
        item_type_in_scope=items_in_scope,
        environment=environment,
        token_credential=credential,
    )

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        try:
            publish_all_items(workspace)
            break
        except Exception as e:
            msg = str(e)
            transient = (
                "previous operation is completed" in msg
                or "TooManyRequests" in msg
                or "operation is in progress" in msg
            )
            if transient and attempt < max_attempts:
                wait = 15 * attempt
                print(
                    f"Transient publish error (attempt {attempt}/{max_attempts}); "
                    f"retrying in {wait}s: {msg}"
                )
                time.sleep(wait)
                continue
            print(f"ERROR: Deployment failed for environment '{environment}': {e}")
            sys.exit(1)

    print(f"Deployment completed successfully for environment: {environment}")


if __name__ == "__main__":
    main()
