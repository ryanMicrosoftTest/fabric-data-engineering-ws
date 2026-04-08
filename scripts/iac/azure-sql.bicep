@description('The name of the SQL server')
param serverName string

@description('The administrator login for the SQL server')
param administratorLogin string

@description('The administrator password for the SQL server')
@secure()
param administratorLoginPassword string

@description('The name of the database to create')
param databaseName string = 'labdb'

@description('Location for all resources')
param location string = 'eastus2'

@description('Tags to apply to all resources')
param tags object = {}

@description('The tier of the particular SKU')
param skuTier string = 'Standard'

@description('The name of the SKU')
param skuName string = 'S0'

@description('The capacity of the particular SKU')
param skuCapacity int = 10

@description('Client IP address for firewall rule')
param clientIpAddress string = ''

@description('Whether to allow Azure services and resources to access this server')
param allowAzureIps bool = true

// SQL Server
resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: serverName
  location: location
  tags: tags
  properties: {
    administratorLogin: administratorLogin
    administratorLoginPassword: administratorLoginPassword
    version: '12.0'
    publicNetworkAccess: 'Enabled'
  }
}

// Database
resource database 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: databaseName
  location: location
  tags: tags
  sku: {
    name: skuName
    tier: skuTier
    capacity: skuCapacity
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 2147483648 // 2GB for Standard S0
    catalogCollation: 'SQL_Latin1_General_CP1_CI_AS'
    zoneRedundant: false
    readScale: 'Disabled'
    requestedBackupStorageRedundancy: 'Local'
  }
}

// Firewall rule to allow Azure services
resource firewallRuleAzure 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = if (allowAzureIps) {
  parent: sqlServer
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// Firewall rule for client IP (if provided)
resource firewallRuleClient 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = if (clientIpAddress != '') {
  parent: sqlServer
  name: 'ClientIpRule'
  properties: {
    startIpAddress: clientIpAddress
    endIpAddress: clientIpAddress
  }
}

// Firewall rule to allow all public internet access
resource firewallRulePublic 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = {
  parent: sqlServer
  name: 'AllowAllPublicIps'
  properties: {
    startIpAddress: '0.0.0.1'
    endIpAddress: '255.255.255.255'
  }
}

// Outputs
output serverFqdn string = sqlServer.properties.fullyQualifiedDomainName
output serverId string = sqlServer.id
output databaseId string = database.id
output connectionString string = 'Server=tcp:${sqlServer.properties.fullyQualifiedDomainName},1433;Initial Catalog=${databaseName};Persist Security Info=False;User ID={username};Password={password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;'
