# Fabric Notebook: Lakehouse Dependency Discovery
# ================================================
# This notebook discovers all artifacts that depend on a target lakehouse
# using the Fabric/Power BI Admin Scanner API, Fabric Items API, and
# notebook definition scanning. Results are saved to a Fabric SQL Database.
#
# Prerequisites:
#   - Run this notebook in a Fabric workspace
#   - The running identity must be a Fabric Admin OR use a service principal with Tenant.Read.All
#   - Metadata scanning must be enabled in tenant settings
#     (Admin Portal → Tenant Settings → Admin API settings → Enhance admin APIs responses)
#   - Target Fabric SQL Database must exist
#
# META {
#   "dependencies": {
#     "lakehouse": {}
#   }
# }

# CELL 1 — Parameters
# ====================
# Configure these parameters before running

# Target lakehouse to analyze
WORKSPACE_ID = ""           # e.g., "a8cbda3d-903e-4154-97d9-9a91c95abb42"
LAKEHOUSE_NAME = ""         # e.g., "bronze_lh" (used for name-based matching)
LAKEHOUSE_ID = ""           # e.g., "7d0dfe91-9b90-42c4-9424-07dbd443a54c"

# Output Fabric SQL Database configuration
SQL_DATABASE_NAME = ""      # e.g., "migration_tracking_db"
SQL_SERVER = ""             # e.g., "xyz123.database.fabric.microsoft.com"
SQL_SCHEMA = "dbo"          # e.g., "dbo" or "migration"
SQL_TABLE = "lakehouse_dependencies"  # table name for results

# Optional: scan additional workspaces (comma-separated GUIDs) 
# Leave empty to scan only the target workspace
ADDITIONAL_WORKSPACE_IDS = ""  # e.g., "guid1,guid2,guid3"

# Scanner API options
INCLUDE_LINEAGE = True
INCLUDE_DATASOURCE_DETAILS = True
INCLUDE_DATASET_SCHEMA = True
INCLUDE_DATASET_EXPRESSIONS = True
INCLUDE_ARTIFACT_USERS = True

# Whether to scan notebook definitions in the workspace for hardcoded references
SCAN_NOTEBOOK_DEFINITIONS = True

# CELL 2 — Imports & Authentication
# ==================================

import requests
import json
import time
import struct
import pyodbc
from datetime import datetime, timezone
from typing import Optional

# Get access tokens using Fabric notebook utilities
# Power BI Admin API token
pbi_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

# Fabric API token
fabric_token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")

# SQL Database token (for Azure AD auth to Fabric SQL Database)
sql_token = mssparkutils.credentials.getToken("https://database.windows.net")

PBI_HEADERS = {
    "Authorization": f"Bearer {pbi_token}",
    "Content-Type": "application/json"
}

FABRIC_HEADERS = {
    "Authorization": f"Bearer {fabric_token}",
    "Content-Type": "application/json"
}

print("✅ Authentication tokens acquired")


# CELL 3 — Helper Functions
# ==========================

def fabric_get(url: str, headers: dict = None) -> dict:
    """Make a GET request to the Fabric or Power BI API with retry logic."""
    hdrs = headers or FABRIC_HEADERS
    for attempt in range(3):
        resp = requests.get(url, headers=hdrs)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 30))
            print(f"  ⏳ Rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception(f"Failed after 3 retries: {url}")


def fabric_post(url: str, body: dict = None, headers: dict = None) -> dict:
    """Make a POST request to the Fabric or Power BI API."""
    hdrs = headers or PBI_HEADERS
    resp = requests.post(url, headers=hdrs, json=body)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 30))
        print(f"  ⏳ Rate limited, waiting {retry_after}s...")
        time.sleep(retry_after)
        resp = requests.post(url, headers=hdrs, json=body)
    resp.raise_for_status()
    return resp.json() if resp.content else {"status_code": resp.status_code}


def get_sql_connection() -> pyodbc.Connection:
    """Create a pyodbc connection to Fabric SQL Database using Azure AD token."""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE_NAME};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    # Encode the token for pyodbc Azure AD auth
    token_bytes = sql_token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    return conn


print("✅ Helper functions defined")


# CELL 4 — Validate Parameters
# ==============================

assert WORKSPACE_ID, "❌ WORKSPACE_ID is required"
assert LAKEHOUSE_ID or LAKEHOUSE_NAME, "❌ Either LAKEHOUSE_ID or LAKEHOUSE_NAME is required"
assert SQL_DATABASE_NAME, "❌ SQL_DATABASE_NAME is required"
assert SQL_SERVER, "❌ SQL_SERVER is required"

# Build list of workspace IDs to scan
workspace_ids = [WORKSPACE_ID]
if ADDITIONAL_WORKSPACE_IDS:
    workspace_ids.extend([w.strip() for w in ADDITIONAL_WORKSPACE_IDS.split(",") if w.strip()])

print(f"🎯 Target lakehouse: {LAKEHOUSE_NAME or LAKEHOUSE_ID}")
print(f"📋 Workspaces to scan: {len(workspace_ids)}")
print(f"💾 Output: {SQL_DATABASE_NAME}.{SQL_SCHEMA}.{SQL_TABLE}")


# CELL 5 — Step 1: List All Items in Workspace
# ===============================================

print("=" * 60)
print("STEP 1: Listing all items in workspace(s)")
print("=" * 60)

all_items = []

for ws_id in workspace_ids:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items"
    while url:
        result = fabric_get(url)
        items = result.get("value", [])
        for item in items:
            item["workspaceId"] = ws_id
        all_items.extend(items)
        
        continuation = result.get("continuationUri")
        url = continuation if continuation else None

print(f"✅ Found {len(all_items)} items across {len(workspace_ids)} workspace(s)")

# Summarize by type
type_counts = {}
for item in all_items:
    t = item.get("type", "Unknown")
    type_counts[t] = type_counts.get(t, 0) + 1

for item_type, count in sorted(type_counts.items()):
    print(f"   {item_type}: {count}")


# CELL 6 — Resolve Lakehouse Identity
# =====================================

# If we only have the name, find the ID (and vice versa)
target_lakehouse = None
for item in all_items:
    if item.get("type") == "Lakehouse":
        if LAKEHOUSE_ID and item.get("id") == LAKEHOUSE_ID:
            target_lakehouse = item
            break
        elif LAKEHOUSE_NAME and item.get("displayName") == LAKEHOUSE_NAME:
            target_lakehouse = item
            break

if target_lakehouse:
    LAKEHOUSE_ID = target_lakehouse["id"]
    LAKEHOUSE_NAME = target_lakehouse["displayName"]
    print(f"✅ Resolved lakehouse: {LAKEHOUSE_NAME} ({LAKEHOUSE_ID})")
else:
    print(f"⚠️ Lakehouse not found in item list. Proceeding with provided values.")
    print(f"   ID: {LAKEHOUSE_ID}, Name: {LAKEHOUSE_NAME}")


# CELL 7 — Step 2: Run Admin Scanner API
# =========================================

print("=" * 60)
print("STEP 2: Running Admin Scanner API (metadata scan)")
print("=" * 60)

# Step 2a: Initiate scan
scan_params = []
if INCLUDE_LINEAGE:
    scan_params.append("lineage=True")
if INCLUDE_DATASOURCE_DETAILS:
    scan_params.append("datasourceDetails=True")
if INCLUDE_DATASET_SCHEMA:
    scan_params.append("datasetSchema=True")
if INCLUDE_DATASET_EXPRESSIONS:
    scan_params.append("datasetExpressions=True")
if INCLUDE_ARTIFACT_USERS:
    scan_params.append("getArtifactUsers=True")

query_string = "&".join(scan_params)
scan_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?{query_string}"

scan_body = {"workspaces": workspace_ids}

print(f"📡 Initiating scan for {len(workspace_ids)} workspace(s)...")
scan_response = fabric_post(scan_url, scan_body, PBI_HEADERS)
scan_id = scan_response.get("id")
print(f"   Scan ID: {scan_id}")

# Step 2b: Poll until scan completes
print("⏳ Waiting for scan to complete...")
status_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}"

max_wait = 300  # 5 minutes
elapsed = 0
while elapsed < max_wait:
    status = fabric_get(status_url, PBI_HEADERS)
    scan_status = status.get("status", "Unknown")
    
    if scan_status == "Succeeded":
        print(f"✅ Scan completed successfully ({elapsed}s)")
        break
    elif scan_status in ("Failed", "Error"):
        raise Exception(f"❌ Scan failed: {json.dumps(status, indent=2)}")
    else:
        time.sleep(5)
        elapsed += 5
        if elapsed % 15 == 0:
            print(f"   Status: {scan_status} ({elapsed}s elapsed)")

if elapsed >= max_wait:
    raise Exception("❌ Scan timed out after 5 minutes")

# Step 2c: Get scan results
print("📥 Retrieving scan results...")
result_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}"
scan_result = fabric_get(result_url, PBI_HEADERS)

print(f"✅ Scan results retrieved for {len(scan_result.get('workspaces', []))} workspace(s)")


# CELL 8 — Step 3: Analyze Dependencies
# ========================================

print("=" * 60)
print("STEP 3: Analyzing dependencies on target lakehouse")
print("=" * 60)

dependencies = []
run_timestamp = datetime.now(timezone.utc).isoformat()

# Search patterns for the target lakehouse
search_patterns = [
    LAKEHOUSE_ID.lower() if LAKEHOUSE_ID else "",
    LAKEHOUSE_NAME.lower() if LAKEHOUSE_NAME else "",
]
search_patterns = [p for p in search_patterns if p]


def add_dependency(
    dep_type: str,
    dep_item_id: str,
    dep_item_name: str,
    dep_item_type: str,
    dep_workspace_id: str,
    relationship: str,
    detail: str,
    confidence: str = "HIGH",
    owner: str = "",
):
    """Add a discovered dependency to the results list."""
    dependencies.append({
        "run_timestamp": run_timestamp,
        "target_lakehouse_id": LAKEHOUSE_ID,
        "target_lakehouse_name": LAKEHOUSE_NAME,
        "target_workspace_id": WORKSPACE_ID,
        "dependency_type": dep_type,
        "dependent_item_id": dep_item_id,
        "dependent_item_name": dep_item_name,
        "dependent_item_type": dep_item_type,
        "dependent_workspace_id": dep_workspace_id,
        "relationship": relationship,
        "detail": detail[:2000] if detail else "",
        "confidence": confidence,
        "owner": owner,
    })


# --- 3a: Analyze Semantic Models (Datasets) ---
print("\n🔍 Scanning semantic models...")
sm_count = 0

for ws in scan_result.get("workspaces", []):
    ws_id = ws.get("id", "")
    
    for dataset in ws.get("datasets", []):
        ds_id = dataset.get("id", "")
        ds_name = dataset.get("name", "")
        configured_by = dataset.get("configuredBy", "")
        
        # Check expressions (Power Query M) for lakehouse references
        for expr in dataset.get("expressions", []):
            expr_text = expr.get("expression", "").lower()
            for pattern in search_patterns:
                if pattern in expr_text:
                    add_dependency(
                        dep_type="SEMANTIC_MODEL",
                        dep_item_id=ds_id,
                        dep_item_name=ds_name,
                        dep_item_type="SemanticModel",
                        dep_workspace_id=ws_id,
                        relationship="DATA_SOURCE",
                        detail=f"Expression '{expr.get('name', '')}' references lakehouse. M expression: {expr.get('expression', '')[:500]}",
                        owner=configured_by,
                    )
                    sm_count += 1
                    break
        
        # Check tables for DirectLake source references
        for table in dataset.get("tables", []):
            for source in table.get("source", []):
                source_expr = source.get("expression", "").lower()
                for pattern in search_patterns:
                    if pattern in source_expr:
                        add_dependency(
                            dep_type="SEMANTIC_MODEL_TABLE",
                            dep_item_id=ds_id,
                            dep_item_name=ds_name,
                            dep_item_type="SemanticModel",
                            dep_workspace_id=ws_id,
                            relationship="DIRECT_LAKE_TABLE",
                            detail=f"Table '{table.get('name', '')}' sources from lakehouse. Expression: {source_expr[:500]}",
                            owner=configured_by,
                        )
                        sm_count += 1
                        break
        
        # Check upstream datasource references
        for ds_usage in dataset.get("datasourceUsages", []):
            ds_instance_id = ds_usage.get("datasourceInstanceId", "")
            # Cross-reference with datasource instances
            for ds_inst in scan_result.get("datasourceInstances", []):
                if ds_inst.get("datasourceId") == ds_instance_id:
                    conn_details = json.dumps(ds_inst.get("connectionDetails", {})).lower()
                    for pattern in search_patterns:
                        if pattern in conn_details:
                            add_dependency(
                                dep_type="SEMANTIC_MODEL",
                                dep_item_id=ds_id,
                                dep_item_name=ds_name,
                                dep_item_type="SemanticModel",
                                dep_workspace_id=ws_id,
                                relationship="DATASOURCE_CONNECTION",
                                detail=f"Datasource connection references lakehouse: {conn_details[:500]}",
                                owner=configured_by,
                            )
                            sm_count += 1
                            break

print(f"   Found {sm_count} semantic model dependencies")


# --- 3b: Analyze Reports ---
print("🔍 Scanning reports...")
rpt_count = 0

for ws in scan_result.get("workspaces", []):
    ws_id = ws.get("id", "")
    
    # Build dataset-to-lakehouse mapping from our dependencies
    dependent_dataset_ids = set(
        d["dependent_item_id"] for d in dependencies
        if d["dependent_item_type"] == "SemanticModel"
    )
    
    for report in ws.get("reports", []):
        rpt_dataset_id = report.get("datasetId", "")
        if rpt_dataset_id in dependent_dataset_ids:
            add_dependency(
                dep_type="REPORT",
                dep_item_id=report.get("id", ""),
                dep_item_name=report.get("name", ""),
                dep_item_type="Report",
                dep_workspace_id=ws_id,
                relationship="BOUND_TO_SEMANTIC_MODEL",
                detail=f"Report bound to semantic model '{rpt_dataset_id}' which depends on lakehouse",
                owner=report.get("modifiedBy", ""),
            )
            rpt_count += 1

print(f"   Found {rpt_count} report dependencies (via semantic models)")


# --- 3c: Analyze Dashboards ---
print("🔍 Scanning dashboards...")
dash_count = 0

for ws in scan_result.get("workspaces", []):
    ws_id = ws.get("id", "")
    
    for dashboard in ws.get("dashboards", []):
        for tile in dashboard.get("tiles", []):
            tile_dataset_id = tile.get("datasetId", "")
            if tile_dataset_id in dependent_dataset_ids:
                add_dependency(
                    dep_type="DASHBOARD",
                    dep_item_id=dashboard.get("id", ""),
                    dep_item_name=dashboard.get("displayName", ""),
                    dep_item_type="Dashboard",
                    dep_workspace_id=ws_id,
                    relationship="TILE_BOUND_TO_SEMANTIC_MODEL",
                    detail=f"Dashboard tile references semantic model '{tile_dataset_id}'",
                )
                dash_count += 1
                break  # One match per dashboard is enough

print(f"   Found {dash_count} dashboard dependencies")


# --- 3d: Analyze Dataflows ---
print("🔍 Scanning dataflows...")
df_count = 0

for ws in scan_result.get("workspaces", []):
    ws_id = ws.get("id", "")
    
    for dataflow in ws.get("dataflows", []):
        for ds_usage in dataflow.get("datasourceUsages", []):
            ds_instance_id = ds_usage.get("datasourceInstanceId", "")
            for ds_inst in scan_result.get("datasourceInstances", []):
                if ds_inst.get("datasourceId") == ds_instance_id:
                    conn_details = json.dumps(ds_inst.get("connectionDetails", {})).lower()
                    for pattern in search_patterns:
                        if pattern in conn_details:
                            add_dependency(
                                dep_type="DATAFLOW",
                                dep_item_id=dataflow.get("objectId", ""),
                                dep_item_name=dataflow.get("name", ""),
                                dep_item_type="Dataflow",
                                dep_workspace_id=ws_id,
                                relationship="DATASOURCE_CONNECTION",
                                detail=f"Dataflow datasource references lakehouse: {conn_details[:500]}",
                                owner=dataflow.get("configuredBy", ""),
                            )
                            df_count += 1
                            break

print(f"   Found {df_count} dataflow dependencies")


# CELL 9 — Step 4: Scan Fabric Items API for Direct References
# ==============================================================

print("\n" + "=" * 60)
print("STEP 4: Scanning Fabric Items for direct lakehouse references")
print("=" * 60)

# --- 4a: Find items that reference the lakehouse by type ---
# Notebooks, Pipelines, etc. that we couldn't catch via Scanner API

print("🔍 Scanning notebooks & pipelines via item definitions...")
nb_count = 0
pl_count = 0

for ws_id in workspace_ids:
    for item in all_items:
        if item.get("workspaceId") != ws_id:
            continue
        
        item_type = item.get("type", "")
        item_id = item.get("id", "")
        item_name = item.get("displayName", "")
        
        if item_type == "Notebook" and SCAN_NOTEBOOK_DEFINITIONS:
            # Try to get the notebook definition to scan for lakehouse references
            try:
                def_url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/notebooks/{item_id}/getDefinition"
                def_resp = requests.post(def_url, headers=FABRIC_HEADERS)
                
                if def_resp.status_code == 200:
                    definition = def_resp.json()
                    # Check the definition parts for lakehouse references
                    for part in definition.get("definition", {}).get("parts", []):
                        payload = part.get("payload", "")
                        # Payload is base64 encoded
                        import base64
                        try:
                            decoded = base64.b64decode(payload).decode("utf-8", errors="ignore").lower()
                        except Exception:
                            decoded = ""
                        
                        for pattern in search_patterns:
                            if pattern in decoded:
                                # Determine the type of reference
                                ref_types = []
                                if "default_lakehouse" in decoded and pattern in decoded:
                                    ref_types.append("DEFAULT_LAKEHOUSE_BINDING")
                                if "abfss://" in decoded and pattern in decoded:
                                    ref_types.append("ABFSS_PATH")
                                if f"spark.sql" in decoded or "savetable" in decoded.replace("_","").replace(" ",""):
                                    ref_types.append("SPARK_SQL")
                                if not ref_types:
                                    ref_types.append("CODE_REFERENCE")
                                
                                for ref_type in ref_types:
                                    add_dependency(
                                        dep_type="NOTEBOOK",
                                        dep_item_id=item_id,
                                        dep_item_name=item_name,
                                        dep_item_type="Notebook",
                                        dep_workspace_id=ws_id,
                                        relationship=ref_type,
                                        detail=f"Notebook code/metadata references lakehouse ({ref_type})",
                                    )
                                    nb_count += 1
                                break
                
                elif def_resp.status_code == 202:
                    # Long-running operation — get Location header and poll
                    location = def_resp.headers.get("Location", "")
                    if location:
                        for poll_attempt in range(6):  # Max 30s
                            time.sleep(5)
                            poll_resp = requests.get(location, headers=FABRIC_HEADERS)
                            if poll_resp.status_code == 200:
                                definition = poll_resp.json()
                                for part in definition.get("definition", {}).get("parts", []):
                                    payload = part.get("payload", "")
                                    import base64
                                    try:
                                        decoded = base64.b64decode(payload).decode("utf-8", errors="ignore").lower()
                                    except Exception:
                                        decoded = ""
                                    
                                    for pattern in search_patterns:
                                        if pattern in decoded:
                                            add_dependency(
                                                dep_type="NOTEBOOK",
                                                dep_item_id=item_id,
                                                dep_item_name=item_name,
                                                dep_item_type="Notebook",
                                                dep_workspace_id=ws_id,
                                                relationship="CODE_REFERENCE",
                                                detail=f"Notebook code/metadata references lakehouse",
                                            )
                                            nb_count += 1
                                            break
                                break
                            elif poll_resp.status_code != 202:
                                break
                
            except Exception as e:
                print(f"   ⚠️ Could not scan notebook '{item_name}': {e}")
        
        elif item_type == "DataPipeline":
            # Try to get the pipeline definition
            try:
                def_url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/dataPipelines/{item_id}/getDefinition"
                def_resp = requests.post(def_url, headers=FABRIC_HEADERS)
                
                if def_resp.status_code == 200:
                    definition = def_resp.json()
                    def_str = json.dumps(definition).lower()
                    
                    for pattern in search_patterns:
                        if pattern in def_str:
                            ref_types = []
                            if "lakehoustable" in def_str.replace("e","").replace(" ","") or "lakehouse" in def_str:
                                ref_types.append("PIPELINE_ACTIVITY")
                            if "artifactid" in def_str.replace(" ",""):
                                ref_types.append("ARTIFACT_REFERENCE")
                            if not ref_types:
                                ref_types.append("PIPELINE_REFERENCE")
                            
                            for ref_type in ref_types:
                                add_dependency(
                                    dep_type="DATA_PIPELINE",
                                    dep_item_id=item_id,
                                    dep_item_name=item_name,
                                    dep_item_type="DataPipeline",
                                    dep_workspace_id=ws_id,
                                    relationship=ref_type,
                                    detail=f"Pipeline definition references lakehouse ({ref_type})",
                                )
                                pl_count += 1
                            break
                
                elif def_resp.status_code == 202:
                    location = def_resp.headers.get("Location", "")
                    if location:
                        for poll_attempt in range(6):
                            time.sleep(5)
                            poll_resp = requests.get(location, headers=FABRIC_HEADERS)
                            if poll_resp.status_code == 200:
                                definition = poll_resp.json()
                                def_str = json.dumps(definition).lower()
                                for pattern in search_patterns:
                                    if pattern in def_str:
                                        add_dependency(
                                            dep_type="DATA_PIPELINE",
                                            dep_item_id=item_id,
                                            dep_item_name=item_name,
                                            dep_item_type="DataPipeline",
                                            dep_workspace_id=ws_id,
                                            relationship="PIPELINE_REFERENCE",
                                            detail=f"Pipeline definition references lakehouse",
                                        )
                                        pl_count += 1
                                        break
                                break
                            elif poll_resp.status_code != 202:
                                break
                                
            except Exception as e:
                print(f"   ⚠️ Could not scan pipeline '{item_name}': {e}")

print(f"   Found {nb_count} notebook dependencies")
print(f"   Found {pl_count} pipeline dependencies")


# CELL 10 — Step 5: Discover Shortcuts
# =======================================

print("\n" + "=" * 60)
print("STEP 5: Discovering OneLake shortcuts")
print("=" * 60)

sc_count = 0

# Check all lakehouses for shortcuts that reference our target
for item in all_items:
    if item.get("type") != "Lakehouse":
        continue
    
    lh_id = item.get("id", "")
    lh_name = item.get("displayName", "")
    lh_ws_id = item.get("workspaceId", "")
    
    try:
        # List shortcuts for this lakehouse
        sc_url = f"https://api.fabric.microsoft.com/v1/workspaces/{lh_ws_id}/items/{lh_id}/shortcuts"
        sc_resp = requests.get(sc_url, headers=FABRIC_HEADERS)
        
        if sc_resp.status_code == 200:
            shortcuts = sc_resp.json().get("value", [])
            for shortcut in shortcuts:
                sc_target = json.dumps(shortcut.get("target", {})).lower()
                for pattern in search_patterns:
                    if pattern in sc_target:
                        direction = "INBOUND" if lh_id != LAKEHOUSE_ID else "OUTBOUND"
                        add_dependency(
                            dep_type="SHORTCUT",
                            dep_item_id=lh_id,
                            dep_item_name=f"{lh_name} → {shortcut.get('name', '')}",
                            dep_item_type="Shortcut",
                            dep_workspace_id=lh_ws_id,
                            relationship=f"SHORTCUT_{direction}",
                            detail=f"Shortcut '{shortcut.get('name', '')}' in lakehouse '{lh_name}' references target lakehouse. Path: {shortcut.get('path', '')}",
                        )
                        sc_count += 1
                        break
    except Exception as e:
        # Shortcuts API may not be available or may return 404 for some lakehouses
        pass

print(f"   Found {sc_count} shortcut dependencies")


# CELL 11 — Step 6: Associate SQL Endpoint Consumers
# =====================================================

print("\n" + "=" * 60)
print("STEP 6: Identifying SQL endpoint associations")
print("=" * 60)

# Every lakehouse has an auto-generated SQL analytics endpoint
# Find the SQL endpoint (SQLEndpoint item type) associated with our lakehouse
sqlep_count = 0

for item in all_items:
    if item.get("type") in ("SQLEndpoint", "SQLDatabase") and item.get("workspaceId") == WORKSPACE_ID:
        item_name = item.get("displayName", "").lower()
        # SQL endpoints often share the lakehouse name
        if LAKEHOUSE_NAME.lower() in item_name:
            add_dependency(
                dep_type="SQL_ENDPOINT",
                dep_item_id=item.get("id", ""),
                dep_item_name=item.get("displayName", ""),
                dep_item_type=item.get("type", ""),
                dep_workspace_id=item.get("workspaceId", ""),
                relationship="SQL_ANALYTICS_ENDPOINT",
                detail=f"SQL analytics endpoint '{item.get('displayName', '')}' is associated with the lakehouse. DirectQuery and external SQL consumers will need connection string updates.",
            )
            sqlep_count += 1

print(f"   Found {sqlep_count} SQL endpoint associations")


# CELL 12 — Summary Report
# ==========================

print("\n" + "=" * 60)
print("DEPENDENCY DISCOVERY SUMMARY")
print("=" * 60)

# Deduplicate by (item_id, relationship)
seen = set()
unique_deps = []
for d in dependencies:
    key = (d["dependent_item_id"], d["relationship"])
    if key not in seen:
        seen.add(key)
        unique_deps.append(d)

dependencies = unique_deps

# Count by type
type_summary = {}
for d in dependencies:
    t = d["dependency_type"]
    type_summary[t] = type_summary.get(t, 0) + 1

print(f"\n🎯 Target: {LAKEHOUSE_NAME} ({LAKEHOUSE_ID})")
print(f"📊 Total unique dependencies found: {len(dependencies)}\n")

for dep_type, count in sorted(type_summary.items()):
    emoji = {
        "SEMANTIC_MODEL": "📊",
        "SEMANTIC_MODEL_TABLE": "📋",
        "REPORT": "📈",
        "DASHBOARD": "📉",
        "NOTEBOOK": "📓",
        "DATA_PIPELINE": "🔧",
        "DATAFLOW": "🔄",
        "SHORTCUT": "🔗",
        "SQL_ENDPOINT": "🗄️",
    }.get(dep_type, "•")
    print(f"   {emoji} {dep_type}: {count}")

print(f"\n{'=' * 60}")


# CELL 13 — Step 7: Create Output Table & Write to SQL Database
# ================================================================

print("=" * 60)
print("STEP 7: Writing results to Fabric SQL Database")
print("=" * 60)

conn = get_sql_connection()
cursor = conn.cursor()

# Create schema if it doesn't exist (skip for dbo)
if SQL_SCHEMA.lower() != "dbo":
    try:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{SQL_SCHEMA}')
            BEGIN
                EXEC('CREATE SCHEMA [{SQL_SCHEMA}]')
            END
        """)
        conn.commit()
        print(f"✅ Schema [{SQL_SCHEMA}] ensured")
    except Exception as e:
        print(f"⚠️ Schema creation note: {e}")

# Create the dependencies table
create_table_sql = f"""
IF OBJECT_ID('[{SQL_SCHEMA}].[{SQL_TABLE}]', 'U') IS NULL
BEGIN
    CREATE TABLE [{SQL_SCHEMA}].[{SQL_TABLE}] (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        run_timestamp           DATETIME2 NOT NULL,
        target_lakehouse_id     NVARCHAR(100) NOT NULL,
        target_lakehouse_name   NVARCHAR(255) NOT NULL,
        target_workspace_id     NVARCHAR(100) NOT NULL,
        dependency_type         NVARCHAR(50) NOT NULL,
        dependent_item_id       NVARCHAR(100) NOT NULL,
        dependent_item_name     NVARCHAR(500) NOT NULL,
        dependent_item_type     NVARCHAR(100) NOT NULL,
        dependent_workspace_id  NVARCHAR(100) NOT NULL,
        relationship            NVARCHAR(100) NOT NULL,
        detail                  NVARCHAR(2000),
        confidence              NVARCHAR(20) DEFAULT 'HIGH',
        owner                   NVARCHAR(255),
        migration_status        NVARCHAR(50) DEFAULT 'NOT_STARTED',
        migration_notes         NVARCHAR(2000),
        migrated_at             DATETIME2
    )
END
"""

cursor.execute(create_table_sql)
conn.commit()
print(f"✅ Table [{SQL_SCHEMA}].[{SQL_TABLE}] ensured")

# Optional: Clear previous results for this lakehouse (keeps history if commented out)
# Uncomment the next 3 lines to replace instead of append:
# cursor.execute(f"DELETE FROM [{SQL_SCHEMA}].[{SQL_TABLE}] WHERE target_lakehouse_id = ?", LAKEHOUSE_ID)
# conn.commit()
# print(f"🗑️ Cleared previous results for {LAKEHOUSE_NAME}")

# Insert dependencies
insert_sql = f"""
INSERT INTO [{SQL_SCHEMA}].[{SQL_TABLE}] (
    run_timestamp, target_lakehouse_id, target_lakehouse_name, target_workspace_id,
    dependency_type, dependent_item_id, dependent_item_name, dependent_item_type,
    dependent_workspace_id, relationship, detail, confidence, owner
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

inserted = 0
for d in dependencies:
    cursor.execute(insert_sql, (
        d["run_timestamp"],
        d["target_lakehouse_id"],
        d["target_lakehouse_name"],
        d["target_workspace_id"],
        d["dependency_type"],
        d["dependent_item_id"],
        d["dependent_item_name"],
        d["dependent_item_type"],
        d["dependent_workspace_id"],
        d["relationship"],
        d["detail"],
        d["confidence"],
        d["owner"],
    ))
    inserted += 1

conn.commit()
print(f"✅ Inserted {inserted} dependency records")


# Create a summary view
view_name = f"{SQL_TABLE}_summary"
view_sql = f"""
IF OBJECT_ID('[{SQL_SCHEMA}].[{view_name}]', 'V') IS NOT NULL
    DROP VIEW [{SQL_SCHEMA}].[{view_name}];
"""
cursor.execute(view_sql)
conn.commit()

view_sql = f"""
CREATE VIEW [{SQL_SCHEMA}].[{view_name}] AS
SELECT 
    target_lakehouse_name,
    dependency_type,
    COUNT(*) AS dependency_count,
    STRING_AGG(dependent_item_name, ', ') AS dependent_items,
    MAX(run_timestamp) AS last_scanned,
    SUM(CASE WHEN migration_status = 'COMPLETED' THEN 1 ELSE 0 END) AS migrated_count,
    SUM(CASE WHEN migration_status = 'NOT_STARTED' THEN 1 ELSE 0 END) AS pending_count
FROM [{SQL_SCHEMA}].[{SQL_TABLE}]
GROUP BY target_lakehouse_name, dependency_type
"""
cursor.execute(view_sql)
conn.commit()
print(f"✅ Created summary view [{SQL_SCHEMA}].[{view_name}]")

cursor.close()
conn.close()

print(f"\n🎉 Discovery complete!")
print(f"   Query your results:  SELECT * FROM [{SQL_SCHEMA}].[{SQL_TABLE}]")
print(f"   Summary view:        SELECT * FROM [{SQL_SCHEMA}].[{view_name}]")
print(f"   Track migration:     UPDATE [{SQL_SCHEMA}].[{SQL_TABLE}] SET migration_status = 'COMPLETED' WHERE dependent_item_id = '<id>'")


# CELL 14 — Display Results as DataFrame (optional)
# =====================================================

# Convert results to a Spark DataFrame for in-notebook viewing
if dependencies:
    deps_df = spark.createDataFrame(dependencies)
    display(
        deps_df.select(
            "dependency_type",
            "dependent_item_name", 
            "dependent_item_type",
            "relationship",
            "confidence",
            "owner"
        ).orderBy("dependency_type", "dependent_item_name")
    )
else:
    print("No dependencies found. Verify your LAKEHOUSE_ID and LAKEHOUSE_NAME parameters.")
