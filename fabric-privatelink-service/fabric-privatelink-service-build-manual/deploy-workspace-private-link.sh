#!/bin/bash
# deploy-workspace-private-link.sh (Manual variant)
# Deploys a Fabric workspace-level Private Link Service + Private Endpoint
# and validates subnet capacity.
#
# NOTE: This variant does NOT set the workspace inbound policy.
# Deny public access must be configured manually via:
#   Fabric Portal > Workspace Settings > Inbound Networking
#
# Usage:
#   ./deploy-workspace-private-link.sh <resource_name> <tenant_id> <workspace_id>

set -euo pipefail

RESOURCE_NAME="${1:?Usage: $0 <resource_name> <tenant_id> <workspace_id>}"
TENANT_ID="${2:?Usage: $0 <resource_name> <tenant_id> <workspace_id>}"
WORKSPACE_ID="${3:?Usage: $0 <resource_name> <tenant_id> <workspace_id>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/config.json"
BICEP_FILE="${SCRIPT_DIR}/fabric-workspace-private-link.bicep"

# --- Helper functions ---

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

fail() { log "ERROR: $*" >&2; exit 1; }

check_prerequisites() {
    command -v az >/dev/null 2>&1 || fail "Azure CLI (az) is not installed."
    command -v jq >/dev/null 2>&1 || fail "jq is not installed."
    az account show >/dev/null 2>&1 || fail "Not logged in to Azure CLI. Run 'az login' first."
    [[ -f "$CONFIG_FILE" ]] || fail "Config file not found: $CONFIG_FILE"
    [[ -f "$BICEP_FILE" ]] || fail "Bicep file not found: $BICEP_FILE"
}

read_config() {
    SUBSCRIPTION_ID=$(jq -r '.subscription_id' "$CONFIG_FILE")
    RESOURCE_GROUP=$(jq -r '.resource_group' "$CONFIG_FILE")
    LOCATION=$(jq -r '.location' "$CONFIG_FILE")
    VNET_NAME=$(jq -r '.vnet.name' "$CONFIG_FILE")
    VNET_RG=$(jq -r '.vnet.resource_group' "$CONFIG_FILE")
    SUBNET_NAME=$(jq -r '.subnet.name' "$CONFIG_FILE")
    RESERVED_IPS=$(jq -r '.subnet.reserved_ips_per_workspace' "$CONFIG_FILE")
    DNS_ZONE_NAME=$(jq -r '.dns_zones.fabric.name' "$CONFIG_FILE")
    DNS_ZONE_RG=$(jq -r '.dns_zones.fabric.resource_group' "$CONFIG_FILE")

    [[ "$SUBSCRIPTION_ID" != "null" && "$SUBSCRIPTION_ID" != "" ]] || fail "subscription_id not set in config.json"
    [[ "$RESOURCE_GROUP" != "null" && "$RESOURCE_GROUP" != "" ]] || fail "resource_group not set in config.json"
    [[ "$VNET_NAME" != "null" && "$VNET_NAME" != "" ]] || fail "vnet.name not set in config.json"
    [[ "$SUBNET_NAME" != "null" && "$SUBNET_NAME" != "" ]] || fail "subnet.name not set in config.json"
}

set_subscription() {
    log "Setting subscription to $SUBSCRIPTION_ID"
    az account set --subscription "$SUBSCRIPTION_ID"
}

check_subnet_capacity() {
    log "Checking subnet capacity..."

    SUBNET_INFO=$(az network vnet subnet show \
        --resource-group "$VNET_RG" \
        --vnet-name "$VNET_NAME" \
        --name "$SUBNET_NAME" \
        --query '{addressPrefix: addressPrefix, ipConfigurations: ipConfigurations}' \
        -o json)

    ADDRESS_PREFIX=$(echo "$SUBNET_INFO" | jq -r '.addressPrefix')
    USED_IPS=$(echo "$SUBNET_INFO" | jq '.ipConfigurations | length')

    # Calculate total IPs from CIDR prefix
    CIDR_BITS="${ADDRESS_PREFIX##*/}"
    TOTAL_IPS=$(( (1 << (32 - CIDR_BITS)) - 5 ))  # Azure reserves 5 IPs per subnet

    AVAILABLE_IPS=$(( TOTAL_IPS - USED_IPS ))

    log "Subnet: $SUBNET_NAME ($ADDRESS_PREFIX)"
    log "  Total usable IPs: $TOTAL_IPS"
    log "  Used IPs: $USED_IPS"
    log "  Available IPs: $AVAILABLE_IPS"
    log "  Required IPs: $RESERVED_IPS"

    if [[ "$AVAILABLE_IPS" -lt "$RESERVED_IPS" ]]; then
        fail "Insufficient subnet capacity. Available: $AVAILABLE_IPS, Required: $RESERVED_IPS. Expand the subnet or use a different one."
    fi

    log "Subnet capacity check passed."
}

deploy_bicep() {
    log "Deploying Bicep template..."

    SUBNET_ID="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${VNET_RG}/providers/Microsoft.Network/virtualNetworks/${VNET_NAME}/subnets/${SUBNET_NAME}"
    DNS_ZONE_ID="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${DNS_ZONE_RG}/providers/Microsoft.Network/privateDnsZones/${DNS_ZONE_NAME}"

    DEPLOYMENT_NAME="fabric-pl-${RESOURCE_NAME}-$(date +%Y%m%d%H%M%S)"

    az deployment group create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$DEPLOYMENT_NAME" \
        --template-file "$BICEP_FILE" \
        --parameters \
            resourceName="$RESOURCE_NAME" \
            workspaceId="$WORKSPACE_ID" \
            tenantId="$TENANT_ID" \
            subnetId="$SUBNET_ID" \
            fabricDnsZoneId="$DNS_ZONE_ID" \
            location="$LOCATION" \
        --query 'properties.outputs' \
        -o json

    log "Bicep deployment completed."
}

validate_private_endpoint() {
    log "Validating private endpoint state..."

    PE_NAME="fabric-pe-${RESOURCE_NAME}"

    PE_STATE=$(az network private-endpoint show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$PE_NAME" \
        --query 'privateLinkServiceConnections[0].privateLinkServiceConnectionState.status' \
        -o tsv 2>/dev/null || echo "NotFound")

    if [[ "$PE_STATE" == "Approved" ]]; then
        log "Private endpoint '$PE_NAME' is in Approved state."
    else
        log "WARNING: Private endpoint '$PE_NAME' is in '$PE_STATE' state. It may need manual approval."
    fi

    # Show allocated private IPs
    log "Private IP addresses:"
    az network private-endpoint show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$PE_NAME" \
        --query 'customDnsConfigs[].{fqdn: fqdn, ipAddresses: ipAddresses}' \
        -o table 2>/dev/null || log "  (Unable to retrieve IP details)"
}

# --- Main ---

main() {
    log "=== Fabric Workspace Private Link Deployment (Manual) ==="
    log "Resource Name: $RESOURCE_NAME"
    log "Tenant ID:     $TENANT_ID"
    log "Workspace ID:  $WORKSPACE_ID"
    log ""

    check_prerequisites
    read_config
    set_subscription
    check_subnet_capacity
    deploy_bicep
    validate_private_endpoint

    log ""
    log "=== Deployment Complete ==="
    log "Private Link Service: fabric-pls-${RESOURCE_NAME}"
    log "Private Endpoint:     fabric-pe-${RESOURCE_NAME}"
    log ""
    log "IMPORTANT: Workspace inbound policy was NOT configured."
    log "To deny public access, go to:"
    log "  Fabric Portal > Workspace Settings > Inbound Networking"
    log "  Select 'Allow connections from selected networks and workspace level private links'"
    log ""
    log "Validate DNS resolution from a machine inside the VNet:"
    log "  nslookup {workspaceid}.z{xy}.w.api.fabric.microsoft.com"
}

main
