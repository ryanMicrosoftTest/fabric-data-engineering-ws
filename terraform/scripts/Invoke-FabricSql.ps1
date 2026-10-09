<#
.SYNOPSIS
    Executes a T-SQL script against a Fabric SQL analytics endpoint using Entra auth.

.DESCRIPTION
    Called by Terraform (null_resource.sql / null_resource.verify) but safe to run
    by hand. Acquires an access token for the SQL resource via the Azure CLI and
    passes it to Invoke-Sqlcmd, avoiding any stored credential.

    The identity running this must be the one that OWNS the consumer lakehouse.
    In delegated identity mode the endpoint impersonates the owner, so applying
    these grants as a different admin can produce a broken ownership chain that
    only fails later, for the consumer, and never for you.

.PARAMETER Server
    SQL analytics endpoint FQDN.

.PARAMETER Database
    Database name (matches the lakehouse display name).

.PARAMETER ScriptPath
    Path to the .sql file to execute.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Server,
    [Parameter(Mandatory)] [string] $Database,
    [Parameter(Mandatory)] [string] $ScriptPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if (-not (Test-Path -LiteralPath $ScriptPath)) {
    throw "Script not found: $ScriptPath"
}

if (-not (Get-Module -ListAvailable -Name SqlServer)) {
    throw "The SqlServer PowerShell module is required. Install with: Install-Module SqlServer -Scope CurrentUser"
}

Import-Module SqlServer -ErrorAction Stop

Write-Host "Acquiring Entra token for https://database.windows.net/ ..."

$tokenJson = az account get-access-token --resource "https://database.windows.net/" --output json 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($tokenJson)) {
    throw "Failed to acquire a token. Run 'az login' (as the identity that owns the consumer lakehouse) and retry."
}

$token = ($tokenJson | ConvertFrom-Json).accessToken

Write-Host "Executing $([System.IO.Path]::GetFileName($ScriptPath)) against $Database @ $Server"

$result = Invoke-Sqlcmd `
    -ServerInstance    $Server `
    -Database          $Database `
    -AccessToken       $token `
    -InputFile         $ScriptPath `
    -QueryTimeout      300 `
    -ConnectionTimeout 60 `
    -ErrorAction       Stop `
    -Verbose

if ($result) { $result | Format-Table -AutoSize | Out-String | Write-Host }

Write-Host "Done."
