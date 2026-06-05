# Fabric Workspace Private Link — Manual Pipeline

Automated Azure DevOps pipeline that deploys a Fabric workspace-level Private Link. The workspace inbound policy (deny public access) is **not** configured by this pipeline and must be set manually.

## What This Pipeline Deploys

1. **Private Link Service** (`Microsoft.Fabric/privateLinkServicesForFabric`) — 1:1 with workspace
2. **Private Endpoint** in the configured subnet with ~5 private IPs
3. **DNS A records** auto-registered in `privatelink.fabric.microsoft.com`

## What This Pipeline Does NOT Do

**Workspace inbound policy is not set.** After deployment, you must manually deny public access:

1. Go to **Fabric Portal** → open the workspace
2. Select **Workspace Settings** → **Inbound Networking**
3. Select **Allow connections from selected networks and workspace level private links**
4. Select **Apply**

> This setting can take up to 30 minutes to take effect.

## Prerequisites

### Fabric Tenant Settings

| Setting | Required Value |
|---------|---------------|
| **Configure workspace-level inbound network rules** | **Enabled** (Admin Portal → Tenant Settings → Advanced Networking) |

### Azure Permissions

- Contributor (or equivalent) on the resource group for PLS/PE resources
- Network Contributor on the VNet/subnet
- Private DNS Zone Contributor on the DNS zone

### Shared Infrastructure (must exist before running)

- A Virtual Network with a subnet sized for private endpoints (10 IPs per workspace)
- Private DNS zone: `privatelink.fabric.microsoft.com`, linked to the VNet
- The `Microsoft.Fabric` resource provider re-registered in the subscription

### Azure DevOps

- A service connection (`azureServiceConnection` variable) with access to the Azure subscription
- Approval groups configured for Security, Networking, and Cloud Team

## Configuration

Edit `config.json` with your environment values:

```json
{
  "subscription_id": "your-subscription-id",
  "resource_group": "rg-for-private-link-resources",
  "location": "eastus2",
  "vnet": {
    "name": "your-vnet-name",
    "resource_group": "rg-containing-vnet"
  },
  "subnet": {
    "name": "your-subnet-for-private-endpoints",
    "reserved_ips_per_workspace": 10
  },
  "dns_zones": {
    "fabric": {
      "name": "privatelink.fabric.microsoft.com",
      "resource_group": "rg-containing-dns-zone"
    }
  }
}
```

## Pipeline Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `resource_name` | Azure resource name (free-form, unique within RG) | `analytics-workspace-prod` |
| `tenant_id` | Microsoft Entra tenant ID | `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee` |
| `workspace_id` | Fabric workspace ID (GUID from workspace URL) | `b196641e-3340-48b6-975f-df7bb9e3aaee` |

> **Note:** `resource_name` is a free-form name — it does not need to match your Fabric tenant or workspace name. A naming convention like `fabric-pls-{team}-{environment}` is recommended.

## Pipeline Stages

| Stage | Description |
|-------|-------------|
| **Validate** | Bicep lint, subnet capacity check, what-if deployment preview |
| **Approve** | Single approval gate — Security, Networking, Cloud Team notified simultaneously |
| **Deploy** | Runs deployment script: creates PLS + PE + DNS records |

## Post-Deployment Validation

From a machine inside the VNet (or a peered VNet), run:

```powershell
# Replace with actual workspace ID (no dashes) and xy prefix (first 2 chars of workspace ID)
nslookup {workspaceid}.z{xy}.w.api.fabric.microsoft.com
```

Expected result: resolves to a `10.x.x.x` private IP address.

If it resolves to a public IP, verify:
1. The Private Endpoint is in **Approved** state in the Azure portal
2. The DNS zone has A records for the workspace
3. The DNS zone is linked to the VNet

## Shared Infrastructure: VNet Link with Fallback to Internet

If your environment has a mix of private and public workspaces, enable **fallback to internet** (`NxDomainRedirect`) on the VNet link. This allows workspaces without a PE to resolve publicly while PE workspaces resolve privately.

Deploy this **once** before creating workspace PEs:

```bash
az deployment group create \
  --resource-group <dns-zone-resource-group> \
  --template-file fabric-privatelink-vnet-link.bicep \
  --parameters \
    linkName="fabric-vnet-link" \
    virtualNetworkId="/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Network/virtualNetworks/<vnet>" \
    enableFallbackToInternet=true
```

Or update an existing VNet link:

```bash
az network private-dns link vnet update \
  --zone-name privatelink.fabric.microsoft.com \
  --resource-group <dns-zone-rg> \
  --name <vnet-link-name> \
  --resolution-policy NxDomainRedirect
```

## Files

| File | Description |
|------|-------------|
| `azure-pipelines.yml` | Azure DevOps YAML pipeline definition |
| `config.json` | Shared infrastructure configuration |
| `fabric-workspace-private-link.bicep` | Bicep template for PLS + PE + DNS |
| `fabric-privatelink-vnet-link.bicep` | Bicep template for VNet link with NxDomainRedirect (shared infra, deploy once) |
| `deploy-workspace-private-link.sh` | Deployment script with validation (no inbound policy) |
