There's no "deploy" — this POC just runs locally on your laptop. Here's the runbook:

One-time prerequisites (per-machine)

 1. Install ODBC Driver 18 for SQL Server if not already present: winget install Microsoft.ODBCDriver.18.SQLServer
 
 Verify: Get-OdbcDriver -Name "ODBC Driver 18 for SQL Server"
 2. Install Python deps: cd C:\Users\rharrington\repos\fabric-data-engineering-ws\on-prem-to-fabric-wh
  py -m venv .venv
  .\.venv\Scripts\Activate.ps1
  pip install -r requirements.txt
 3. Azure login (so DefaultAzureCredential can read Key Vault as your user): az login

One-time setup in Fabric (verify these are done)

 - ✅ Warehouse exists (on-prem-warehouse-test-wh)
 - ✅ SPN added to workspace as Member or Contributor — workspace a8cbda3d-903e-4154-97d9-9a91c95abb42 
   Workspace Contributor/Member implicitly grants full read/write/DDL on every warehouse in the workspace — no T-SQL needed.
   (Fabric Warehouse does NOT support `CREATE USER ... FROM EXTERNAL PROVIDER` or `ALTER ROLE db_datareader ADD MEMBER`. If you need item-level
   instead of workspace-level access, share the warehouse with the SPN and use `GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON SCHEMA::dbo TO [<spn-display-name>]; GRANT CREATE TABLE TO [<spn-display-name>];` directly — skip CREATE USER.)
 - ✅ Tenant setting "Service principals can use Fabric APIs" enabled (Admin portal → Tenant settings)

Run the POC

 cd C:\Users\rharrington\repos\fabric-data-engineering-ws\on-prem-to-fabric-wh
 .\.venv\Scripts\Activate.ps1
 py scripts\write_to_warehouse.py --rows 100

Expected output: SUCCESS: wrote 100 rows to [dbo].[synthetic_orders]

Verify in Fabric

Open the warehouse in the portal → SQL editor →

 SELECT TOP 10 * FROM dbo.synthetic_orders;
 SELECT COUNT(*) FROM dbo.synthetic_orders;

If anything fails, see docs\troubleshooting.md.


