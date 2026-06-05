# Fabric Private Endpoint VNet Setup — OneLake DFS Private DNS Resolution

**Date:** 2026-02-17  
**Environment:** Azure Subscription `ME-MngEnvMCAP372892-rharrington-1` (910ebf13-1058-405d-b6cf-eda03e5288d1)

---

## Objective

Enable private connectivity from VM `azure-priv-ws-vm` (VNet: `fabric-private-ws-vnet`) to a Microsoft Fabric workspace over Azure Private Endpoints, so that OneLake data access via `abfss://` resolves to private IPs instead of routing over the public internet.

---

## Architecture

```
VM (azure-priv-ws-vm)                    Fabric Workspace
  VNet: fabric-private-ws-vnet            b196641e-3340-48b6-975f-df7bb9e3aaee
  Subnet: default (10.0.0.0/24)
       │
       ├── Private Endpoint: fabric-ws-pe (10.0.0.5–10.0.0.9)
       │     └── Target: Microsoft.Fabric/privateLinkServicesForFabric/fabric-private-workspace
       │
       └── Bastion: fabric-private-ws-vnet-bastion (Basic SKU)
             Subnet: AzureBastionSubnet (10.0.1.0/26)
```

---

## Problem

The private endpoint `fabric-ws-pe` was correctly provisioned and had DNS A records created across three Private DNS Zones. However, **none of the zones were linked to `fabric-private-ws-vnet`** — they were only linked to a different VNet (`vnet-1` in resource group `test-PL`).

This caused all DNS resolution from the VM to bypass the private zones and resolve to public IPs, even though the private endpoint was healthy and approved.

---

## DNS Resolution Chains

Fabric private endpoints use multiple DNS resolution paths depending on the service being accessed. Each path goes through a different Private DNS Zone:

### 1. Fabric Workspace API

```
b196641e-...z0a.w.api.fabric.microsoft.com
  → api.privatelink.analysis.windows.net        ← Zone: privatelink.analysis.windows.net
    → (public IP if zone not linked)
    → 10.0.0.5 (private IP if zone linked)
```

### 2. OneLake Data Path (DFS)

```
onelake.dfs.fabric.microsoft.com
  → onelake.pbidedicated.windows.net
    → onelake.privatelink.pbidedicated.windows.net  ← Zone: privatelink.pbidedicated.windows.net
      → (public IP if zone not linked)
      → 10.0.0.7 (private IP if zone linked)
```

### 3. Fabric Services (API, DFS, Blob, OneLake, Compute)

```
b196641e...zb1.dfs.privatelink.fabric.microsoft.com  ← Zone: privatelink.fabric.microsoft.com
  → 10.0.0.8
```

---

## Private DNS Zones & VNet Links Required

A single Fabric workspace private endpoint creates records across **three** Private DNS Zones. Each must be independently linked to every VNet that needs private access.

| DNS Zone | Resource Group | Purpose | A Record Example | Private IP |
|----------|---------------|---------|-----------------|------------|
| `privatelink.analysis.windows.net` | test-pl | Fabric workspace API | `api` | 10.0.0.5 |
| `privatelink.fabric.microsoft.com` | fabric-rg | Fabric services (API, DFS, blob, OneLake, compute) | `b196641e...zb1.dfs` | 10.0.0.5–10.0.0.9 |
| `privatelink.pbidedicated.windows.net` | test-pl | OneLake data path (`onelake.dfs.fabric.microsoft.com`) | `onelake` | 10.0.0.7 |

> **Key Insight:** Missing a VNet link on **any one** of these zones causes that specific traffic path to resolve publicly, even if the private endpoint itself is correctly configured and approved.

---

## Changes Made

### 1. Linked `privatelink.analysis.windows.net` to `fabric-private-ws-vnet`

```powershell
az network private-dns link vnet create `
  --zone-name privatelink.analysis.windows.net `
  --resource-group test-pl `
  --name fabric-vnet-link `
  --virtual-network /subscriptions/910ebf13-1058-405d-b6cf-eda03e5288d1/resourceGroups/fabric-rg/providers/Microsoft.Network/virtualNetworks/fabric-private-ws-vnet `
  --registration-enabled false
```

**Before:** `nslookup b196641e-...z0a.w.api.fabric.microsoft.com` → public IP `20.41.4.111`  
**After:** Resolves to private IP `10.0.0.5`

### 2. Linked `privatelink.pbidedicated.windows.net` to `fabric-private-ws-vnet`

```powershell
az network private-dns link vnet create `
  --zone-name privatelink.pbidedicated.windows.net `
  --resource-group test-pl `
  --name fabric-vnet-link `
  --virtual-network /subscriptions/910ebf13-1058-405d-b6cf-eda03e5288d1/resourceGroups/fabric-rg/providers/Microsoft.Network/virtualNetworks/fabric-private-ws-vnet `
  --registration-enabled false
```

**Before:** `nslookup onelake.dfs.fabric.microsoft.com` → public IP `20.41.5.6`  
**After:** Resolves to private IP (`10.x.x.x`)

### 3. `privatelink.fabric.microsoft.com` — Already Linked

This zone (resource group: `fabric-rg`) was already linked to `fabric-private-ws-vnet`. No action needed.

---

## Validation

From the VM (`azure-priv-ws-vm`), run:

```powershell
# Validate Fabric API resolves privately
nslookup b196641e-3340-48b6-975f-df7bb9e3aaee.z0a.w.api.fabric.microsoft.com

# Validate OneLake DFS resolves privately
nslookup onelake.dfs.fabric.microsoft.com
```

Both should return `10.x.x.x` private IPs. If either returns a public IP, verify the corresponding Private DNS Zone has a VNet link to `fabric-private-ws-vnet`.

---

## abfss:// Path Format

When connecting to OneLake from the VM, use the standard public FQDN — private routing is handled transparently at the DNS level:

```
abfss://b196641e-3340-48b6-975f-df7bb9e3aaee@onelake.dfs.fabric.microsoft.com/0a69b54c-a7ff-4e66-b136-9ed481d43f83/Tables/<table_name>
```

> **Do not** use workspace-specific FQDNs (e.g., `b196641e-...z0a.w.dfs.fabric.microsoft.com`) in `abfss://` paths — the driver does not recognize that URL pattern.
