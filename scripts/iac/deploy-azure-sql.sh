#!/bin/bash

# ============================================================
# Azure SQL Server Deployment Script
# Deploys Azure SQL Server and Database using Bicep template
# ============================================================

set -e

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BICEP_FILE="$SCRIPT_DIR/azure-sql.bicep"
PARAMETERS_FILE="$SCRIPT_DIR/azure-sql.parameters.json"
SQL_SCRIPT="$SCRIPT_DIR/setup-database.sql"
CONFIG_FILE="$SCRIPT_DIR/iac.config"

# Load credentials from config file
if [[ -f "$CONFIG_FILE" ]]; then
    source "$CONFIG_FILE"
else
    echo "ERROR: Config file not found: $CONFIG_FILE"
    echo "Copy iac.config.template to iac.config and fill in your credentials."
    exit 1
fi

# Default values (can be overridden by environment variables)
RESOURCE_GROUP_NAME="${RESOURCE_GROUP_NAME:-rg-lab-sql-eastus2}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-sql-deployment-$(date +%Y%m%d-%H%M%S)}"
LOCATION="${LOCATION:-eastus2}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Azure CLI is installed and logged in
check_azure_cli() {
    if ! command -v az &> /dev/null; then
        print_error "Azure CLI is not installed. Please install it first."
        exit 1
    fi
    
    if ! az account show &> /dev/null; then
        print_error "Not logged into Azure. Please run 'az login' first."
        exit 1
    fi
    
    print_success "Azure CLI is ready"
}

# Function to get current public IP
get_public_ip() {
    local ip
    ip=$(curl -s https://api.ipify.org 2>/dev/null || curl -s https://ifconfig.me 2>/dev/null || echo "")
    if [[ $ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "$ip"
    else
        print_warning "Could not determine public IP automatically"
        echo ""
    fi
}

# Function to create resource group
create_resource_group() {
    print_status "Creating resource group: $RESOURCE_GROUP_NAME"
    
    if az group show --name "$RESOURCE_GROUP_NAME" &> /dev/null; then
        print_success "Resource group already exists"
    else
        az group create \
            --name "$RESOURCE_GROUP_NAME" \
            --location "$LOCATION" \
            --output table
        print_success "Resource group created successfully"
    fi
}

# Function to validate Bicep template
validate_template() {
    print_status "Validating Bicep template..."
    
    local params="serverName=lab-sql-server-001"
    params="$params administratorLogin=$SQL_ADMIN_LOGIN"
    params="$params administratorLoginPassword=$SQL_ADMIN_PASSWORD"
    params="$params databaseName=labdb"
    params="$params location=$LOCATION"
    params="$params skuTier=Standard"
    params="$params skuName=S0"
    params="$params skuCapacity=10"
    params="$params allowAzureIps=true"
    
    if [[ -n "$CLIENT_IP" ]]; then
        params="$params clientIpAddress=$CLIENT_IP"
    fi
    
    az deployment group validate \
        --resource-group "$RESOURCE_GROUP_NAME" \
        --template-file "$BICEP_FILE" \
        --parameters $params \
        --output table
        
    if [[ $? -eq 0 ]]; then
        print_success "Template validation passed"
    else
        print_error "Template validation failed"
        exit 1
    fi
}

# Function to deploy infrastructure
deploy_infrastructure() {
    print_status "Deploying Azure SQL infrastructure..."
    print_status "Deployment name: $DEPLOYMENT_NAME"
    
    local params="serverName=lab-sql-server-001"
    params="$params administratorLogin=$SQL_ADMIN_LOGIN"
    params="$params administratorLoginPassword=$SQL_ADMIN_PASSWORD"
    params="$params databaseName=labdb"
    params="$params location=$LOCATION"
    params="$params skuTier=Standard"
    params="$params skuName=S0"
    params="$params skuCapacity=10"
    params="$params allowAzureIps=true"
    
    if [[ -n "$CLIENT_IP" ]]; then
        params="$params clientIpAddress=$CLIENT_IP"
        print_status "Using client IP: $CLIENT_IP"
    else
        print_warning "No client IP specified - only Azure services will have access"
    fi
    
    az deployment group create \
        --resource-group "$RESOURCE_GROUP_NAME" \
        --name "$DEPLOYMENT_NAME" \
        --template-file "$BICEP_FILE" \
        --parameters $params \
        --output table
        
    if [[ $? -eq 0 ]]; then
        print_success "Infrastructure deployment completed successfully"
    else
        print_error "Infrastructure deployment failed"
        exit 1
    fi
}

# Function to get deployment outputs
get_deployment_outputs() {
    print_status "Retrieving deployment outputs..."
    
    local server_fqdn=$(az deployment group show \
        --resource-group "$RESOURCE_GROUP_NAME" \
        --name "$DEPLOYMENT_NAME" \
        --query "properties.outputs.serverFqdn.value" \
        --output tsv)
    
    echo ""
    echo "============================================================"
    echo "DEPLOYMENT COMPLETE"
    echo "============================================================"
    echo "Server FQDN: $server_fqdn"
    echo "Database: labdb"
    echo "Admin User: $SQL_ADMIN_LOGIN"
    echo "Admin Password: (see iac.config)"
    echo "Authentication: SQL Server Authentication"
    echo "============================================================"
    echo ""
    
    return 0
}

# Function to run SQL setup script
setup_database() {
    local server_fqdn="$1"
    
    print_status "Next: Set up database users..."
    echo ""
    echo "To create the 25 read-only users, run this command:"
    echo ""
    echo "sqlcmd -S $server_fqdn -d master -U $SQL_ADMIN_LOGIN -P '<see iac.config>' -v ReaderPasswordSuffix=\"$READER_PASSWORD_SUFFIX\" -i '$SQL_SCRIPT'"
    echo ""
    echo "Or use Azure Data Studio / SSMS with the SQL script: $SQL_SCRIPT"
    echo ""
}

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -g, --resource-group    Resource group name (default: $RESOURCE_GROUP_NAME)"
    echo "  -n, --deployment-name   Deployment name (default: auto-generated)"
    echo "  -l, --location         Azure location (default: $LOCATION)"
    echo "  -i, --ip               Client IP address for firewall (default: auto-detect)"
    echo "  --validate-only        Only validate the template, don't deploy"
    echo "  -h, --help             Show this help message"
    echo ""
}

# Main function
main() {
    local validate_only=false
    CLIENT_IP=""
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -g|--resource-group)
                RESOURCE_GROUP_NAME="$2"
                shift 2
                ;;
            -n|--deployment-name)
                DEPLOYMENT_NAME="$2"
                shift 2
                ;;
            -l|--location)
                LOCATION="$2"
                shift 2
                ;;
            -i|--ip)
                CLIENT_IP="$2"
                shift 2
                ;;
            --validate-only)
                validate_only=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    echo "============================================================"
    echo "Azure SQL Server Deployment"
    echo "============================================================"
    echo "Resource Group: $RESOURCE_GROUP_NAME"
    echo "Location: $LOCATION"
    echo "Deployment: $DEPLOYMENT_NAME"
    echo "============================================================"
    echo ""
    
    # Check prerequisites
    check_azure_cli
    
    # Get current IP if not provided
    if [[ -z "$CLIENT_IP" ]]; then
        CLIENT_IP=$(get_public_ip)
        if [[ -n "$CLIENT_IP" ]]; then
            print_status "Auto-detected client IP: $CLIENT_IP"
        fi
    else
        print_status "Using provided client IP: $CLIENT_IP"
    fi
    
    # Create resource group
    create_resource_group
    
    # Validate template
    validate_template
    
    if [[ "$validate_only" == true ]]; then
        print_success "Validation completed successfully - no deployment performed"
        exit 0
    fi
    
    # Deploy infrastructure
    deploy_infrastructure
    
    # Get and display outputs
    server_fqdn=$(az deployment group show \
        --resource-group "$RESOURCE_GROUP_NAME" \
        --name "$DEPLOYMENT_NAME" \
        --query "properties.outputs.serverFqdn.value" \
        --output tsv 2>/dev/null)
    
    if [[ -n "$server_fqdn" ]]; then
        get_deployment_outputs
        setup_database "$server_fqdn"
    else
        print_warning "Could not retrieve server FQDN from deployment outputs"
    fi
    
    print_success "Deployment script completed!"
    echo ""
    echo "Next Steps:"
    echo "1. Run the SQL setup script to create users"
    echo "2. Test connection with read-only users"
    echo "3. Check user credentials in: $SCRIPT_DIR/user-credentials.md"
    echo ""
}

# Run main function with all arguments
main "$@"