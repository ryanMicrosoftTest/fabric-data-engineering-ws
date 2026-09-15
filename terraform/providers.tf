terraform {
  required_version = ">= 1.6.0"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }

  # Backend is configured via -backend-config at init time
  # (resource_group_name, storage_account_name, container_name, key),
  # matching the convention in fabric-cluster-start-time-service/iac.
  backend "azurerm" {}
}

provider "fabric" {
  tenant_id = var.tenant_id

  # `fabric_onelake_data_access_security` is a preview resource and is refused
  # by the provider unless preview mode is explicitly enabled.
  preview = true
}
