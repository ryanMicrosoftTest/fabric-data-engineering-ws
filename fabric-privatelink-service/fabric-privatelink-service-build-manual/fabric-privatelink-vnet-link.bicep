// fabric-privatelink-vnet-link.bicep
// Configures a VNet link on a Private DNS zone with optional NxDomainRedirect
// (fallback to internet) for mixed private/public workspace scenarios.
//
// Deploy this once as shared infrastructure before creating workspace PEs.

@description('Name of the Private DNS zone (e.g., privatelink.fabric.microsoft.com)')
param dnsZoneName string = 'privatelink.fabric.microsoft.com'

@description('Name for the VNet link resource')
param linkName string

@description('Resource ID of the Virtual Network to link')
param virtualNetworkId string

@description('Enable fallback to internet (NxDomainRedirect) for non-PE workspaces')
param enableFallbackToInternet bool = true

@description('Enable auto-registration of VM DNS records (typically false for privatelink zones)')
param registrationEnabled bool = false

resource dnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' existing = {
  name: dnsZoneName
}

resource vnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: dnsZone
  name: linkName
  location: 'global'
  properties: {
    virtualNetwork: {
      id: virtualNetworkId
    }
    registrationEnabled: registrationEnabled
    resolutionPolicy: enableFallbackToInternet ? 'NxDomainRedirect' : 'Default'
  }
}

output vnetLinkName string = vnetLink.name
output vnetLinkId string = vnetLink.id
output resolutionPolicy string = vnetLink.properties.resolutionPolicy
