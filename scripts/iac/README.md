# Azure SQL Database Deployment Guide

This guide provides instructions for deploying an Azure SQL Database with 60 concurrent read-only users for simple SELECT queries.

## Architecture Overview

- **Azure SQL Server**: Standard S0 tier (10 DTUs, ~$15/month)
- **Database**: `labdb` with sample data for testing
- **Users**: 60 read-only users (`reader01` through `reader60`)
- **Location**: East US 2
- **Network**: Public access with firewall rules

## Prerequisites

1. **Azure CLI**: Install and login to Azure CLI
   ```bash
   az login
   ```

2. **Permissions**: Ensure you have Contributor access to create resources

3. **Resource Group**: The script will create one if it doesn't exist

## Deployment Steps

### 1. Navigate to IAC Directory
```bash
cd scripts/iac
```

### 2. Make Deployment Script Executable
```bash
chmod +x deploy-azure-sql.sh
```

### 3. Review Parameters
Edit `azure-sql.parameters.json` to customize:
- Server name
- Administrator credentials
- Tags and other settings

### 4. Deploy Infrastructure
```bash
# Deploy with auto-detected IP
./deploy-azure-sql.sh

# Deploy with custom settings
./deploy-azure-sql.sh -g "my-resource-group" -l "eastus2" -i "192.168.1.100"

# Validate template only
./deploy-azure-sql.sh --validate-only
```

### 5. Configure Database and Users
After infrastructure deployment, run the SQL setup scripts in two steps:

**Step 1: Create SQL logins (run against master database)**
```bash
# Using sqlcmd
sqlcmd -S [your-server-name].database.windows.net -d master -U sqladmin -P '<PASSWORD>' -i setup-database.sql

# Or using Azure Data Studio/SSMS: Connect to master database and run setup-database.sql
```

**Step 2: Create users and sample data (run against labdb database)**
```bash
# Using sqlcmd
sqlcmd -S [your-server-name].database.windows.net -d labdb -U sqladmin -P '<PASSWORD>' -i setup-database-labdb.sql

# Or using Azure Data Studio/SSMS: Connect to labdb database and run setup-database-labdb.sql
```

## Configuration Files

### 1. azure-sql.bicep
Infrastructure as Code template that creates:
- Azure SQL Server with public access
- SQL Database (Standard S0 tier)
- Firewall rules for Azure services and client IP
- Outputs for connection details

### 2. azure-sql.parameters.json
Configuration parameters including:
- Server and database names
- Administrator credentials
- SKU settings
- Network configuration

### 3. setup-database.sql
SQL script for master database that:
- Creates 60 SQL Server logins (reader01-reader60)
- Sets up authentication credentials
- Must be run against master database first

### 4. setup-database-labdb.sql
SQL script for labdb database that:
- Creates sample table with test data
- Creates 60 database users from the logins
- Assigns read-only permissions (db_datareader role)
- Must be run against labdb database after step 3

### 5. deploy-azure-sql.sh
Deployment automation script that:
- Validates prerequisites
- Auto-detects client IP
- Deploys infrastructure using Bicep
- Provides connection information

## Cost Estimation

| Component | Tier | Monthly Cost (USD) |
|-----------|------|-------------------|
| SQL Database | Standard S0 (10 DTUs) | ~$15.00 |
| SQL Server | No additional cost | $0.00 |
| **Total** | | **~$15.00** |

*Prices are estimates for East US 2 region and may vary*

## Security Configuration

### Network Access
- Public endpoint enabled for internet access
- Firewall rules for Azure services and specific IP addresses
- SSL/TLS encryption enforced for all connections

### Authentication
- SQL Server Authentication (not Azure AD)
- Strong password policy enforced
- Read-only permissions for user accounts

### Monitoring
- Azure SQL Database metrics available in Azure Portal
- Query performance insights included
- 7-day automated backups with point-in-time restore

## Performance Considerations

### Standard S0 Specifications
- **DTUs**: 10 (Database Transaction Units)
- **Max Database Size**: 250 GB
- **Max Concurrent Workers**: 200
- **Max Concurrent Logins**: 200

### Optimization Tips for 60 Concurrent Users
1. **Connection Pooling**: Use application-level connection pooling
2. **Query Optimization**: Keep SELECT queries simple and indexed
3. **Monitoring**: Watch DTU usage in Azure Portal
4. **Scaling**: Upgrade to S1 (20 DTUs) if performance issues occur

## Testing Your Deployment

### 1. Test Server Connectivity
```bash
# Test connectivity (replace with your server name)
telnet [your-server-name].database.windows.net 1433
```

### 2. Verify Setup Completed Successfully
```bash
# Test that logins were created
sqlcmd -S [your-server-name].database.windows.net -d master -U sqladmin -P '<PASSWORD>' -Q "SELECT name FROM sys.server_principals WHERE name LIKE 'reader%'"

# Test that users were created in labdb
sqlcmd -S [your-server-name].database.windows.net -d labdb -U sqladmin -P '<PASSWORD>' -Q "SELECT name FROM sys.database_principals WHERE name LIKE 'reader%'"
```

### 3. Test User Authentication
Use any SQL client to connect with:
- **Server**: [your-server-name].database.windows.net
- **Database**: labdb
- **Username**: reader01 (or any reader01-reader60)
- **Password**: <PASSWORD> (corresponding password)

### 4. Test Read Permissions
```sql
-- This should work
SELECT * FROM SampleData;

-- This should fail (read-only user)
INSERT INTO SampleData (Name, Description) VALUES ('Test', 'Should fail');
```

**Quick Test Command:**
```bash
# Test read access with reader01
sqlcmd -S [your-server-name].database.windows.net -d labdb -U reader01 -P '<PASSWORD>' -Q "SELECT COUNT(*) as RecordCount FROM SampleData"
```

## Maintenance Tasks

### Regular Maintenance
1. **Monitor DTU Usage**: Check performance metrics weekly
2. **Review Firewall Rules**: Update IP addresses as needed
3. **Password Rotation**: Consider changing default passwords
4. **Backup Verification**: Verify automated backups are working

### Scaling Options
If you need more performance:
1. **Scale Up**: Upgrade to Standard S1 (20 DTUs) or S2 (50 DTUs)
2. **Connection Limits**: Standard S0 supports 200 concurrent connections
3. **Storage**: Automatically scales up to 250 GB for Standard tier

## Troubleshooting

### Common Issues

1. **"USE statement is not supported" Error**
   - This is an Azure SQL Database limitation
   - Use separate connections for master and labdb databases
   - Run setup-database.sql against master, then setup-database-labdb.sql against labdb

2. **Connection Timeout**
   - Verify firewall rules include your IP
   - Check if Azure services access is enabled

3. **Authentication Failed**
   - Verify username/password combination
   - Check if user exists in database
   - Ensure logins were created in master database first

4. **Permission Denied**
   - Verify user is member of db_datareader role
   - Check object-level permissions
   - Make sure database users were created after logins

5. **High DTU Usage**
   - Monitor query performance in Azure Portal
   - Consider scaling up to higher tier
   - Optimize queries for better performance

### Getting Help

1. **Azure Portal**: Monitor metrics and diagnostics
2. **Query Performance Insight**: Available in Azure Portal
3. **Azure Support**: Create support ticket if needed

## Cleanup

To remove all resources:
```bash
# Delete the entire resource group (WARNING: This deletes everything)
az group delete --name "rg-lab-sql-eastus2" --yes --no-wait
```

## Next Steps

1. **Customize Sample Data**: Replace SampleData table with your actual data
2. **Implement Connection Pooling**: Configure in your applications
3. **Set Up Monitoring**: Create alerts for DTU usage and connection issues
4. **Security Hardening**: Change default passwords and review firewall rules
5. **Backup Strategy**: Configure long-term retention if needed