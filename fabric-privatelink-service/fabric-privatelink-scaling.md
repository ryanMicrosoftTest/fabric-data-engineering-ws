# Scaling Workspace-Level Private Links in Microsoft Fabric

**Date:** 2026-03-16
**Context:** Extending a single-workspace private link setup to support many Fabric workspaces

---

## Background

Setting up workspace-level private links for a single Fabric workspace involves several manual steps:

1. Creating a Private Link Service (`Microsoft.Fabric/privateLinkServicesForFabric`)
2. Creating a Private Endpoint in a VNet targeting that PLS
3. Configuring Private DNS zone records for name resolution
4. Optionally denying public access to the workspace

This document describes how to make that process scalable across many workspaces.

---

## Architecture: Shared vs. Per-Workspace Resources

### Shared Infrastructure (set up once)

These resources are provisioned once and reused across all workspace private endpoints:

- **Virtual Network and subnets** — shared across all PEs
- **Private DNS zones** — records are added per PE, but the zones themselves are shared
  - `privatelink.fabric.microsoft.com` (documented for workspace-level)
  - `privatelink.analysis.windows.net` (may be needed — see [DNS Zone Notes](#dns-zone-notes))
  - `privatelink.pbidedicated.windows.net` (may be needed — see [DNS Zone Notes](#dns-zone-notes))
- **VNet links** — each DNS zone linked to the VNet (one-time setup)
- **Bastion / Jump Box** — for private access to validate connectivity

### Per-Workspace Resources (repeat for each workspace)

Each Fabric workspace requires its own set of:

| Resource | Type | Notes |
|----------|------|-------|
| Private Link Service | `Microsoft.Fabric/privateLinkServicesForFabric` | 1:1 relationship with workspace |
| Private Endpoint | `Microsoft.Network/privateEndpoints` | ~5 IPs allocated per PE (reserve 10) |
| DNS A records | Auto-created in private DNS zones | Created by the PE DNS zone group |
| Workspace inbound policy | Fabric REST API or portal | Optional — deny public access per workspace |

---

## DNS Zone Notes

The workspace-level private links documentation (Step 5, DNS tab) only references `privatelink.fabric.microsoft.com`. However, in practice, the following DNS resolution chains may require additional zones:

| DNS Zone | Documented For | Resolution Chain |
|----------|---------------|-----------------|
| `privatelink.fabric.microsoft.com` | Workspace-level ✅ | `{workspaceid}.z{xy}.*.fabric.microsoft.com` → private IP |
| `privatelink.analysis.windows.net` | Tenant-level only | Workspace API may CNAME through this zone |
| `privatelink.pbidedicated.windows.net` | Tenant-level only | `onelake.dfs.fabric.microsoft.com` resolves through this zone |
| `privatelink.prod.powerquery.microsoft.com` | Tenant-level only | Power Query / Dataflow resolution |

> **Important:** The need for `analysis.windows.net` and `pbidedicated.windows.net` zones for workspace-level private links is not called out in Microsoft's workspace-level documentation. However, real-world deployments have required these zones for full OneLake DFS and workspace API resolution. Validate in your environment whether these zones are needed.

---

## Tenant Settings Required

The following Fabric tenant settings must be configured before workspace-level private links can be used:

| Setting | Location | Required? | Value |
|---------|----------|-----------|-------|
| **Configure workspace-level inbound network rules** | Admin Portal → Tenant Settings → Advanced Networking | ✅ Required | **Enabled** |
| **Configure workspace-level IP firewall** | Admin Portal → Tenant Settings → Advanced Networking | ❌ Optional | Enable for IP-based restrictions |

> **Note:** The tenant-level `Azure Private Link` and `Block Public Internet Access` settings are **not required** for workspace-level private links. These two implementations are independent. If both are enabled, workspace-level takes precedence for workspaces that have a workspace-level PE configured.

---

## Capacity Planning

| Concern | Limit | Planning Guidance |
|---------|-------|-------------------|
| IPs per workspace PE | ~5 (docs recommend reserving 10) | 10 IPs × N workspaces |
| Max PEs per workspace | 100 | Multiple VNets can connect to one workspace |
| Max PLS per tenant | 500 | One PLS per workspace |
| PLS creation rate | 10 per minute | Throttle deployments accordingly |
| Subnet sizing | Based on workspace count | 50 workspaces → `/23` subnet (500+ IPs) |

---

## Bicep Template: Per-Workspace Module

The following Bicep module encapsulates the per-workspace resources. Deploy it once per workspace.

```bicep
// fabric-workspace-private-link.bicep

@description('Short name for the workspace (used in resource naming)')
param workspaceName string

@description('Fabric workspace ID (GUID)')
param workspaceId string

@description('Microsoft Entra tenant ID')
param tenantId string

@description('Resource ID of the subnet for the private endpoint')
param subnetId string

@description('Resource ID of the privatelink.fabric.microsoft.com DNS zone')
param fabricDnsZoneId string

@description('Azure region for the private endpoint')
param location string = resourceGroup().location

// 1. Private Link Service (1:1 with workspace)
resource pls 'Microsoft.Fabric/privateLinkServicesForFabric@2024-06-01' = {
  name: 'fabric-pls-${workspaceName}'
  location: 'global'
  properties: {
    tenantId: tenantId
    workspaceId: workspaceId
  }
}

// 2. Private Endpoint
resource pe 'Microsoft.Network/privateEndpoints@2023-11-01' = {
  name: 'fabric-pe-${workspaceName}'
  location: location
  properties: {
    subnet: {
      id: subnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'fabric-plsc-${workspaceName}'
        properties: {
          privateLinkServiceId: pls.id
          groupIds: [
            'workspace'
          ]
        }
      }
    ]
  }
}

// 3. DNS Zone Group (auto-registers A records in the private DNS zone)
resource dnsGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2023-11-01' = {
  parent: pe
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'privatelink-fabric-microsoft-com'
        properties: {
          privateDnsZoneId: fabricDnsZoneId
        }
      }
    ]
  }
}

output privateEndpointId string = pe.id
output privateEndpointName string = pe.name
output privateLinkServiceId string = pls.id
```

---

## Deployment Script

Use a loop to deploy the module for each workspace:

```bash
#!/bin/bash
# deploy-workspace-private-links.sh

TENANT_ID="<your-tenant-id>"
RESOURCE_GROUP="<your-resource-group>"
SUBNET_ID="<your-subnet-resource-id>"
FABRIC_DNS_ZONE_ID="<privatelink.fabric.microsoft.com-zone-resource-id>"

# Define workspaces: name:workspaceId
WORKSPACES=(
  "analytics-ws:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  "engineering-ws:ffffffff-1111-2222-3333-444444444444"
  "reporting-ws:55555555-6666-7777-8888-999999999999"
)

for ws in "${WORKSPACES[@]}"; do
  NAME="${ws%%:*}"
  WS_ID="${ws##*:}"

  echo "Deploying private link for workspace: $NAME ($WS_ID)"

  az deployment group create \
    --resource-group "$RESOURCE_GROUP" \
    --template-file fabric-workspace-private-link.bicep \
    --parameters \
      workspaceName="$NAME" \
      workspaceId="$WS_ID" \
      tenantId="$TENANT_ID" \
      subnetId="$SUBNET_ID" \
      fabricDnsZoneId="$FABRIC_DNS_ZONE_ID" \
    --no-wait

  # Respect the 10 PLS per minute rate limit
  sleep 7
done
```

---

## Automating Workspace Access Policy (Deny Public Access)

Use the Fabric REST API to restrict inbound public access per workspace, rather than configuring each workspace manually through the portal:

```bash
# Set workspace to deny public access
WORKSPACE_ID="<workspace-id>"
TOKEN=$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)

curl -X PUT \
  "https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID/networkCommunicationPolicy" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "inboundPolicy": {
      "publicNetworkAccess": "Deny"
    }
  }'
```

---

## Validation

After deploying each workspace's private link, validate DNS resolution from a machine inside the VNet:

```powershell
# Replace with actual workspace ID (no dashes) and xy prefix
nslookup {workspaceid}.z{xy}.w.api.fabric.microsoft.com

# Expected: resolves to a 10.x.x.x private IP
```

If any workspace resolves to a public IP, verify:
1. The PE is in an **Approved** connection state
2. The private DNS zone has A records for that workspace
3. The DNS zone is linked to the VNet

---

## DNS Architecture for Mixed Private/Public Workspaces

When some workspaces use private endpoints and others remain publicly accessible, DNS resolution must handle both scenarios. There are two patterns depending on where your clients are located.

### Pattern A: Azure-Only Clients (Recommended)

Use Azure Private DNS Zone with **fallback to internet** (`NxDomainRedirect`). No VMs or additional infrastructure required.

**How it works:**

```
Workspace WITH PE:
  {workspaceid}.z{xy}.dfs.fabric.microsoft.com
    → CNAME to *.privatelink.fabric.microsoft.com
    → A record found in Private DNS Zone → private IP ✅

Workspace WITHOUT PE:
  {workspaceid}.z{xy}.dfs.fabric.microsoft.com
    → CNAME to *.privatelink.fabric.microsoft.com
    → NXDOMAIN in Private DNS Zone (no A record)
    → Fallback to internet → public IP ✅
```

**Setup:** Enable `NxDomainRedirect` on the VNet link for the Private DNS zone:

```bash
# Enable fallback to internet on existing VNet link
az network private-dns link vnet update \
  --zone-name privatelink.fabric.microsoft.com \
  --resource-group <dns-zone-rg> \
  --name <vnet-link-name> \
  --resolution-policy NxDomainRedirect
```

Or deploy via Bicep (see `fabric-privatelink-vnet-link.bicep` in the pipeline directories).

**Key points:**
- A records auto-register when PEs are created (via `privateDnsZoneGroups` in the Bicep template)
- A records auto-cleanup when PEs are deleted
- No per-workspace DNS maintenance required
- No forwarder VMs needed — this is a fully managed Azure feature
- Requires API version 2024-06-01 or higher

### Pattern B: Hybrid / On-Premises Clients

If on-premises clients (e.g., users on corporate VPN not routing through Azure VNet) need to resolve Fabric workspace FQDNs to private IPs, they need a way to query the Azure Private DNS Zone.

**Option 1: Azure DNS Private Resolver (Recommended)**

A managed Azure service — no VMs to maintain:

```
On-premises DNS server
  → Conditional forwarder for fabric.microsoft.com
    → Azure DNS Private Resolver (inbound endpoint in VNet)
      → Azure Private DNS Zone → private IP (if PE exists)
      → Fallback to internet → public IP (if no PE)
```

**Option 2: DNS Forwarder VMs**

Self-managed VMs that forward DNS queries to Azure's recursive resolver (168.63.129.16):

```
On-premises DNS server
  → Conditional forwarder for fabric.microsoft.com
    → DNS Forwarder VMs (in VNet)
      → Azure recursive resolver (168.63.129.16)
        → Azure Private DNS Zone → private IP or fallback
```

**Considerations for Option 2:**
- Requires 2+ VMs for high availability
- Operational overhead: patching, monitoring, scaling
- Additional cost vs. managed DNS Private Resolver
- Security surface area to maintain

### DNS Forwarder VMs vs. Azure DNS Private Resolver

| Factor | DNS Forwarder VMs | Azure DNS Private Resolver |
|--------|-------------------|---------------------------|
| Management | Self-managed (patching, HA) | Fully managed by Azure |
| High Availability | Manual (2+ VMs, load balancer) | Built-in |
| Cost | VM compute costs | Per-endpoint pricing |
| Setup Complexity | Medium-High | Low |
| Conditional Forwarding | Manual configuration | Native rulesets |

### When You Do NOT Need Forwarder VMs or DNS Private Resolver

If all clients accessing Fabric workspaces are Azure VMs (or resources in peered VNets), you only need:
1. A centralized `privatelink.fabric.microsoft.com` Private DNS Zone
2. VNet links (with `NxDomainRedirect` enabled) to each VNet that needs resolution
3. The per-workspace PE pipeline (which auto-registers DNS records)

This is **Pattern A** — the simplest and most cost-effective approach.

---

## References

- [Workspace-level private links overview](https://learn.microsoft.com/en-us/fabric/security/security-workspace-level-private-links-overview)
- [Workspace-level private links setup](https://learn.microsoft.com/en-us/fabric/security/security-workspace-level-private-links-set-up)
- [Supported scenarios and limitations](https://learn.microsoft.com/en-us/fabric/security/security-workspace-level-private-links-support)
- [Tenant-level private links setup](https://learn.microsoft.com/en-us/fabric/security/security-private-links-use)
- [Enable workspace inbound access protection](https://learn.microsoft.com/en-us/fabric/security/security-workspace-enable-inbound-access-protection)
- [Fallback to internet for Azure Private DNS zones](https://learn.microsoft.com/en-us/azure/dns/private-dns-fallback)
- [Azure DNS Private Resolver overview](https://learn.microsoft.com/en-us/azure/dns/dns-private-resolver-overview)
