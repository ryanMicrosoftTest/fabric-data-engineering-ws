// fabric-workspace-private-link.bicep
// Deploys a Fabric workspace-level Private Link Service, Private Endpoint,
// and DNS zone group for automatic A record registration.

@description('Azure resource name for the Private Link Service (free-form, unique within RG)')
param resourceName string

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
  name: 'fabric-pls-${resourceName}'
  location: 'global'
  properties: {
    tenantId: tenantId
    workspaceId: workspaceId
  }
}

// 2. Private Endpoint targeting the PLS
resource pe 'Microsoft.Network/privateEndpoints@2023-11-01' = {
  name: 'fabric-pe-${resourceName}'
  location: location
  properties: {
    subnet: {
      id: subnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'fabric-plsc-${resourceName}'
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

// 3. DNS Zone Group — auto-registers A records in the private DNS zone
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

output privateLinkServiceName string = pls.name
output privateLinkServiceId string = pls.id
output privateEndpointName string = pe.name
output privateEndpointId string = pe.id
