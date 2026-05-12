terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Backend is configured via -backend-config in the deployment pipeline
  # (e.g., resource_group_name, storage_account_name, container_name, key).
  backend "azurerm" {}
}

provider "azurerm" {
  features {}
}
