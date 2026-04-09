"""Authenticate via Service Principal and deploy Fabric items.

Usage:
    python authenticate_spn.py <environment> <config_file_path>

Environment variables required:
    AZURE_TENANT_ID      - Azure AD tenant ID
    AZURE_CLIENT_ID      - Service principal client ID
    AZURE_CLIENT_SECRET  - Service principal client secret
"""

import os
import sys

from azure.identity import ClientSecretCredential
from fabric_cicd import deploy_with_config


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

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    result = deploy_with_config(
        config_file_path=config_file_path,
        environment=environment,
        token_credential=credential,
    )

    print(f"Deployment status: {result.status}")
    print(f"Deployment message: {result.message}")


if __name__ == "__main__":
    main()
