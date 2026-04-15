# Fabric Lakehouse Migration Playbook: Non-Schema → Schema-Enabled

> **Purpose:** A general-purpose, reusable playbook for migrating a Microsoft Fabric Lakehouse from the legacy flat (non-schema) layout to a schema-enabled lakehouse with hybrid medallion + domain schemas.
>
> **Audience:** Data engineering managers, platform engineers, and Fabric administrators responsible for organizational data stores.
>
> **Migration Strategy:** Blue/Green (zero-downtime). The existing lakehouse ("Blue") remains authoritative until the new schema-enabled lakehouse ("Green") is validated and cut over.

---

## Table of Contents

- [Why Migrate?](#why-migrate)
- [Key Constraint: No In-Place Upgrade](#key-constraint-no-in-place-upgrade)
- [Phase 0: Discovery & Assessment](#phase-0-discovery--assessment)
- [Phase 1: Target Architecture & Compatibility Strategy](#phase-1-target-architecture--compatibility-strategy)
- [Phase 2: Security Design](#phase-2-security-design)
- [Phase 3: Capacity & Cost Planning](#phase-3-capacity--cost-planning)
- [Phase 4: Provision Green Lakehouse](#phase-4-provision-green-lakehouse)
- [Phase 5: Data Migration — Backfill & Incremental Sync](#phase-5-data-migration--backfill--incremental-sync)
- [Phase 6: Artifact Migration — Notebooks](#phase-6-artifact-migration--notebooks)
- [Phase 7: Artifact Migration — Pipelines & Dataflows](#phase-7-artifact-migration--pipelines--dataflows)
- [Phase 8: Artifact Migration — Semantic Models & Reports](#phase-8-artifact-migration--semantic-models--reports)
- [Phase 9: Artifact Migration — Shortcuts, SQL Endpoints & External Consumers](#phase-9-artifact-migration--shortcuts-sql-endpoints--external-consumers)
- [Phase 10: CI/CD Pipeline Updates](#phase-10-cicd-pipeline-updates)
- [Phase 11: Validation & Rehearsal](#phase-11-validation--rehearsal)
- [Phase 12: Cutover](#phase-12-cutover)
- [Phase 13: Rollback Plan](#phase-13-rollback-plan)
- [Phase 14: Post-Migration Governance](#phase-14-post-migration-governance)
- [Fabric-Specific Gotchas](#fabric-specific-gotchas)
- [Decision Register](#decision-register)
- [Appendix: Schema Naming Convention](#appendix-schema-naming-convention)
- [Appendix: Discovery Queries](#appendix-discovery-queries)
- [Appendix: Validation Checklist](#appendix-validation-checklist)

---

## Why Migrate?

Schema-enabled lakehouses unlock capabilities that flat lakehouses cannot provide:

| Capability | Non-Schema | Schema-Enabled |
|---|:---:|:---:|
| Organize tables by domain | ❌ Flat list | ✅ Named schemas |
| Schema-level access control (RLS/CLS) | ❌ | ✅ |
| Cross-workspace Spark SQL (four-part naming) | ❌ | ✅ |
| Schema shortcuts (map schema → external folder) | ❌ | ✅ |
| Materialized Lake Views | ❌ | ✅ |
| Table count scalability | Degrades in UI | Organized by schema |

For organizations with 50+ tables in a single lakehouse, the flat layout becomes unmanageable and prevents fine-grained access control. Schema-enabled lakehouses are now the **default** for new lakehouses in Fabric.

---

## Key Constraint: No In-Place Upgrade

> **Microsoft states:** *"Migration tools that convert a non-schema lakehouse to a schema-enabled lakehouse without moving data or downtime aren't yet available."*
>
> — [Fabric Docs: Enable schemas for existing lakehouses](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas#enable-schemas-for-existing-lakehouses)

This means you **must create a new schema-enabled lakehouse** and migrate data into it. There is no toggle to enable schemas on an existing lakehouse. This is the fundamental reason the blue/green approach is required.

**Preparatory shortcut:** You can start using four-part naming (`workspace.lakehouse.dbo.table`) in your existing code today, even against non-schema lakehouses. Spark accepts this syntax and it will make your eventual cutover smoother.

---

## Phase 0: Discovery & Assessment

**Goal:** Build a complete picture of what depends on the source lakehouse so nothing is missed during migration.

### 0.1 — Artifact Dependency Inventory

Create a spreadsheet or tracking table with every artifact that references the source lakehouse. Include:

| Category | What to Find | Where to Look |
|---|---|---|
| **Notebooks** | Default lakehouse binding (METADATA block), Spark SQL table refs, path-based refs (`Tables/`, `Files/`), ABFSS paths | `notebook-content.py` files, `# META` blocks |
| **Data Pipelines** | LakehouseTable sources/sinks, Lookup activities, Copy activities, artifact IDs | Pipeline JSON definitions |
| **Semantic Models** | DirectLake entityName/schemaName, Import connections, DirectQuery SQL endpoint refs | `.tmdl` files, expression sources |
| **Reports / Dashboards / Apps** | Bound semantic models (indirect dependency) | Power BI service |
| **Dataflows (Gen2)** | Lakehouse destination configuration | Dataflow definitions |
| **Shortcuts** | Inbound shortcuts (pointing INTO the lakehouse), outbound shortcuts (FROM the lakehouse) | OneLake Explorer, shortcut metadata tables |
| **SQL Analytics Endpoint Consumers** | Ad hoc SQL users, DirectQuery tools, external BI tools, views, stored procedures | SQL endpoint connection logs, workspace access logs |
| **External Consumers** | Python apps, data science notebooks, partner tools using ABFSS/OneLake APIs | Codebase search for ABFSS URIs |
| **Spark Job Definitions / Environments** | Library/environment dependencies tied to the lakehouse | Environment configurations |
| **Service Principals / Cloud Connections** | Automated jobs, CI/CD pipelines, scheduled refreshes | Workspace settings, Key Vault |
| **Monitoring & Alerts** | Alerts referencing old artifact IDs or lakehouse names | Monitoring hub, alert rules |
| **Subscriptions & Email Alerts** | Report subscriptions tied to affected semantic models | Power BI service subscriptions |

### 0.2 — Data Profile

For each table in the source lakehouse:

- **Table name** and current implicit schema (`dbo`)
- **Row count** and **storage size** (Delta log)
- **Partition strategy** (if any)
- **Last modified date** (Delta history)
- **Write frequency** (how often is it updated? batch vs streaming?)
- **Read frequency** (how many notebooks/pipelines read it?)

```python
# Example: Profile all tables
for table in spark.catalog.listTables("source_lakehouse"):
    df = spark.table(f"source_lakehouse.{table.name}")
    print(f"{table.name}: {df.count()} rows")
```

### 0.3 — Automated Dependency Discovery

Use the **Lakehouse Dependency Discovery Notebook** (`lakehouse_dependency_discovery_nb.py` in this folder) to automate the full inventory. This notebook:

1. **Lists all workspace items** via the Fabric Items API
2. **Runs the Admin Scanner API** (`PostWorkspaceInfo` with lineage, datasource details, schema, and expressions) to discover semantic model, report, dashboard, and dataflow dependencies
3. **Scans notebook definitions** via the Fabric Get Definition API, decoding base64 payloads to find hardcoded lakehouse references (ABFSS paths, METADATA blocks, Spark SQL)
4. **Scans pipeline definitions** for artifact ID and LakehouseTable references
5. **Discovers shortcuts** via the OneLake Shortcuts API across all lakehouses
6. **Identifies SQL endpoint associations**
7. **Writes all results** to a Fabric SQL Database table with columns for dependency type, relationship, detail, confidence, owner, and migration tracking status

**Prerequisites:**
- Running identity must be a Fabric Admin (or service principal with `Tenant.Read.All`)
- Metadata scanning enabled in tenant settings (Admin Portal → Tenant Settings → Admin API settings)
- Target Fabric SQL Database must exist

**APIs Used:**

| API | Purpose | Endpoint |
|---|---|---|
| Fabric Items API | List all workspace items | `GET /v1/workspaces/{id}/items` |
| Admin Scanner API | Full metadata scan with lineage | `POST /v1.0/myorg/admin/workspaces/getInfo` |
| Scanner Status/Result | Poll and retrieve scan results | `GET /v1.0/myorg/admin/workspaces/scanStatus/{id}` |
| Get Notebook Definition | Scan notebook code for references | `POST /v1/workspaces/{id}/notebooks/{id}/getDefinition` |
| Get Pipeline Definition | Scan pipeline JSON for references | `POST /v1/workspaces/{id}/dataPipelines/{id}/getDefinition` |
| OneLake Shortcuts | Discover shortcut dependencies | `GET /v1/workspaces/{id}/items/{id}/shortcuts` |

The output table includes a `migration_status` column (default: `NOT_STARTED`) so you can track progress through the migration:

```sql
-- View all dependencies
SELECT * FROM [dbo].[lakehouse_dependencies] ORDER BY dependency_type;

-- Summary view
SELECT * FROM [dbo].[lakehouse_dependencies_summary];

-- Track migration progress
UPDATE [dbo].[lakehouse_dependencies] 
SET migration_status = 'COMPLETED', migrated_at = GETUTCDATE()
WHERE dependent_item_id = '<item-id>';
```

### 0.4 — Manual Reference Pattern Audit

Supplement the automated discovery with a codebase search for references the APIs may miss (e.g., in Git-managed artifact definitions):

```bash
# Hardcoded lakehouse IDs (GUIDs)
grep -rn "abfss://" fabric_items/
grep -rn "default_lakehouse" fabric_items/

# Table references without schema qualification
grep -rn "spark.sql\|spark.table\|saveAsTable\|\.load(" fabric_items/**/*.py

# Pipeline artifact IDs
grep -rn "artifactId" fabric_items/**/*.json
```

### 0.5 — Risk Assessment Matrix

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Missed artifact dependency | Medium | High | Run discovery notebook + manual grep audit |
| Data drift during parallel operation | Medium | High | Single-write-truth architecture |
| DirectLake framing failure after rebind | Medium | Medium | Pre-validate semantic model owner permissions |
| Capacity spike during parallel operation | High | Medium | Pre-scale or stagger migration |
| Schema name collision with reserved words | Low | Low | Validate names against Fabric rules |
| Shortcut breakage | Medium | High | Recreate shortcuts, validate before cutover |

---

## Phase 1: Target Architecture & Compatibility Strategy

**Goal:** Define the schema layout, naming conventions, and an indirection layer that makes cutover reversible and future changes cheaper.

### 1.1 — Hybrid Medallion + Domain Schema Design

The recommended naming pattern combines medallion layers with business domains:

```
bronze_sales          -- Raw ingestion: sales domain
bronze_finance        -- Raw ingestion: finance domain
bronze_hr             -- Raw ingestion: HR domain
silver_sales          -- Cleansed/conformed: sales domain
silver_finance        -- Cleansed/conformed: finance domain
gold_analytics        -- Aggregated/business-ready: analytics
gold_reporting        -- Curated for Power BI consumption
staging               -- Temporary/intermediate tables
metadata              -- Pipeline metadata, watermarks, audit logs
dbo                   -- Default schema (auto-created, use for legacy/misc)
```

> **Schema naming rules:** Letters, numbers, and underscores only. No spaces, hyphens, or special characters.

### 1.2 — Table-to-Schema Mapping

Create a mapping document for every existing table:

| Current Table Name | Target Schema | Target Table Name | Notes |
|---|---|---|---|
| `fact_sale` | `gold_reporting` | `fact_sale` | DirectLake model dependency |
| `dim_customer` | `gold_reporting` | `dim_customer` | — |
| `raw_orders` | `bronze_sales` | `raw_orders` | — |
| `stg_orders` | `staging` | `stg_orders` | Temporary, consider if needed |

### 1.3 — Indirection & Compatibility Strategy

This is the section that makes the migration reversible and reusable. Define these standards before writing any migration code:

#### Four-Part Naming Adoption
Adopt `workspace.lakehouse.schema.table` naming in ALL new and migrated code. This decouples code from the default lakehouse binding and makes future lakehouse changes trivial.

```python
# BEFORE (fragile — depends on default lakehouse)
df = spark.sql("SELECT * FROM fact_sale")

# AFTER (explicit — works regardless of default lakehouse)
df = spark.sql("SELECT * FROM my_workspace.sales_lakehouse.gold_reporting.fact_sale")
```

#### Notebook Metadata Handling
Notebook `# META` blocks contain hardcoded `default_lakehouse` and `default_lakehouse_name` GUIDs. Strategy options:

| Option | Pros | Cons | Recommendation |
|---|---|---|---|
| **A: Update in CI/CD** — rewrite METADATA during deployment | Automated, environment-aware | Requires CI/CD maturity | ✅ Recommended for teams with CI/CD |
| **B: Remove default lakehouse** — use four-part naming exclusively | No metadata dependency | Requires code changes everywhere | Good for new notebooks |
| **C: Manual update** — change default lakehouse in Fabric portal | Simple | Error-prone, not scalable | ❌ Avoid |

#### Parameterization Standards
Use Fabric Variable Libraries or notebook widgets to parameterize lakehouse references:

```python
# Using Variable Library
lakehouse_name = spark.conf.get("spark.custom.lakehouse_name", "default_lakehouse")
schema_name = spark.conf.get("spark.custom.schema_name", "dbo")

# Using notebook widgets / pipeline parameters
dbutils.widgets.text("lakehouse_name", "sales_lakehouse")
dbutils.widgets.text("schema_name", "gold_reporting")
```

#### Semantic Model Rebinding Rules
- DirectLake models: Update `entityName` AND `schemaName` in TMDL
- Import models: Update Power Query connection M expressions
- DirectQuery models: Update SQL endpoint connection string (if lakehouse changes, the SQL endpoint changes too)

### 1.4 — Decision: One Lakehouse with Many Schemas vs. Multiple Lakehouses

| Criteria | Single Lakehouse + Schemas | Multiple Lakehouses |
|---|---|---|
| Cross-domain queries | Easy (same lakehouse) | Requires shortcuts or four-part naming |
| Access control | Schema-level granularity | Workspace-level granularity |
| Capacity isolation | Shared | Can isolate on different capacities |
| Operational simplicity | Simpler | More items to manage |
| **Recommendation** | ✅ Default choice for most orgs | Use when capacity isolation or strict blast-radius is needed |

---

## Phase 2: Security Design

**Goal:** Design schema-level security before provisioning, because retrofitting security is harder than building it in.

### 2.1 — OneLake Data Access Roles

Schema-enabled lakehouses support OneLake data access roles that can be scoped to specific schemas:

- **ReadAll** — Read access to all schemas (workspace-level)
- **Schema-scoped roles** — Grant read/write to specific schemas only
- Map your existing access groups to the new schema structure

### 2.2 — Row-Level Security (RLS) and Column-Level Security (CLS)

If you use RLS/CLS on any tables, document the current rules and plan how they map to the new schema layout. RLS/CLS rules may reference table names that will change.

### 2.3 — Semantic Model Security Considerations

| Model Type | Security Mechanism | Migration Impact |
|---|---|---|
| DirectLake on OneLake | OneLake security roles, SSO | Must grant model owner access to Green lakehouse |
| DirectLake on SQL endpoint | SQL endpoint permissions + fallback | SQL endpoint changes when lakehouse changes |
| Import | Not affected by lakehouse security | Refresh identity needs access |
| DirectQuery | SQL endpoint permissions | Connection string changes |

**Critical:** The semantic model **owner** (or fixed identity) must have permissions on the Green lakehouse. If the owner lacks access, DirectLake framing will fail silently and reports will show errors. Validate this before cutover.

### 2.4 — Service Principal & Automation Access

- CI/CD service principals need permissions on both Blue and Green during transition
- Scheduled refresh identities need Green lakehouse access
- Cloud connections may need updating
- External tool credentials (if using ABFSS) need new lakehouse path

### 2.5 — Workspace Sharing Limitation

> **Known limitation:** Schema-enabled lakehouses cannot currently be shared directly through workspace-level sharing. Use shortcuts in a lakehouse where the user has a workspace role as a workaround.

Audit current sharing patterns and plan for this limitation before cutover.

---

## Phase 3: Capacity & Cost Planning

**Goal:** Ensure the Fabric capacity can handle parallel operation of Blue and Green lakehouses without degrading production workloads.

### 3.1 — Baseline Capacity Measurement

Before starting, measure current capacity utilization:
- **CU consumption** — average and peak during typical workload
- **Memory utilization** — especially for DirectLake semantic models
- **Spark session concurrency** — typical parallel notebook/job count
- **Storage consumption** — OneLake storage for the source lakehouse

### 3.2 — Migration CU Profile

The migration will temporarily increase load due to:
- Spark jobs copying data from Blue → Green (initial backfill)
- Incremental sync jobs running in parallel with production workloads
- Dual semantic model refresh (both Blue and Green models)
- Validation workloads (Great Expectations, row count comparisons)
- Increased report traffic during validation by testers

### 3.3 — Capacity Strategy

| Option | When to Use |
|---|---|
| **Temporary scale-up** | Increase capacity SKU during migration window, scale back after |
| **Staggered migration** | Migrate domain-by-domain to spread load |
| **Off-hours migration** | Run heavy Spark jobs during low-traffic periods |
| **Dedicated migration capacity** | For very large estates, use a separate Fabric capacity |

### 3.4 — Storage Cost

Schema-enabled lakehouses store data in the same Delta format, so storage cost per table is unchanged. However, during parallel operation you will have **two copies of the data**, temporarily doubling OneLake storage for migrated tables.

Estimate: `Current lakehouse storage × 2` for the duration of parallel operation.

---

## Phase 4: Provision Green Lakehouse

**Goal:** Create the new schema-enabled lakehouse and set up the target schema structure.

### 4.1 — Create via REST API (Recommended for Automation)

```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses

{
  "displayName": "sales_lakehouse_v2",
  "description": "Schema-enabled lakehouse - migration from sales_lakehouse",
  "creationPayload": {
    "enableSchemas": true
  }
}
```

Or create via the Fabric portal with the **Lakehouse schemas** checkbox enabled (it is on by default for new lakehouses).

### 4.2 — Create Schemas

Create each target schema via Spark SQL or the Fabric UI:

```sql
-- Run in a notebook attached to the Green lakehouse
CREATE SCHEMA IF NOT EXISTS bronze_sales;
CREATE SCHEMA IF NOT EXISTS bronze_finance;
CREATE SCHEMA IF NOT EXISTS bronze_hr;
CREATE SCHEMA IF NOT EXISTS silver_sales;
CREATE SCHEMA IF NOT EXISTS silver_finance;
CREATE SCHEMA IF NOT EXISTS gold_analytics;
CREATE SCHEMA IF NOT EXISTS gold_reporting;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS metadata;
-- dbo is auto-created
```

### 4.3 — Configure Security

Apply the security design from Phase 2:
- Create OneLake data access roles scoped to schemas
- Configure RLS/CLS rules on target schemas
- Grant service principal access
- Validate semantic model owner access

### 4.4 — Set Up Schema Shortcuts (If Applicable)

If you plan to use schema shortcuts (mapping a schema to external Delta tables in ADLS Gen2 or another lakehouse), create them now:

1. In Lakehouse Explorer → hover over Tables → **New schema shortcut**
2. Select source (another Fabric lakehouse schema or ADLS Gen2 folder)
3. Validate that referenced tables appear under the new schema

---

## Phase 5: Data Migration — Backfill & Incremental Sync

**Goal:** Copy all data from Blue to Green and keep them in sync until cutover. This is the operationally hardest phase.

### 5.1 — Architecture: Single Write Truth

> **Critical principle:** During the transition period, **Blue remains the single system of record for writes.** All production notebooks and pipelines continue writing to Blue. Green receives data only via sync.

This avoids the nightmare of bidirectional sync and makes rollback safe.

```
┌─────────────┐     sync     ┌─────────────┐
│  Blue (Old)  │ ──────────→ │ Green (New)  │
│  Non-Schema  │             │ Schema-Enabled│
│  WRITES HERE │             │  READ-ONLY   │
└─────────────┘             └─────────────┘
       ↑                           ↑
  Production                  Validation
  Workloads                   & Testing
```

### 5.2 — Initial Backfill

For each table, copy all data from Blue to Green:

```python
# Parameterized backfill notebook
tables_to_migrate = [
    {"source": "fact_sale",     "target_schema": "gold_reporting", "target_table": "fact_sale"},
    {"source": "dim_customer",  "target_schema": "gold_reporting", "target_table": "dim_customer"},
    {"source": "raw_orders",    "target_schema": "bronze_sales",   "target_table": "raw_orders"},
    # ... add all tables
]

for t in tables_to_migrate:
    df = spark.table(f"blue_lakehouse.{t['source']}")
    df.write.mode("overwrite").saveAsTable(f"{t['target_schema']}.{t['target_table']}")
    print(f"Backfilled {t['source']} → {t['target_schema']}.{t['target_table']}: {df.count()} rows")
```

**For large tables** (billions of rows), consider:
- Partition-by-partition copy to manage memory
- `DEEP CLONE` if supported
- Writing with `repartition()` to optimize file sizes

### 5.3 — Incremental Sync

After backfill, keep Green up-to-date with a watermark-based or CDC-based incremental sync:

#### Option A: Watermark-Based (Simpler)

```python
# Runs on schedule (e.g., every 15 minutes)
for t in tables_to_migrate:
    watermark = get_watermark(t['target_table'])  # from metadata table
    
    new_data = spark.sql(f"""
        SELECT * FROM blue_lakehouse.{t['source']}
        WHERE modified_date > '{watermark}'
    """)
    
    if new_data.count() > 0:
        # MERGE into Green
        target = DeltaTable.forName(spark, f"{t['target_schema']}.{t['target_table']}")
        target.alias("t").merge(
            new_data.alias("s"),
            f"t.{t.get('pk', 'id')} = s.{t.get('pk', 'id')}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        update_watermark(t['target_table'], new_data.agg(max("modified_date")).collect()[0][0])
```

#### Option B: Delta Change Data Feed (Preferred if Available)

```python
# Enable CDF on source tables first
# ALTER TABLE blue_lakehouse.fact_sale SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_synced_version) \
    .table("blue_lakehouse.fact_sale")

# Apply changes to Green
# Filter for inserts, updates, deletes and apply accordingly
```

#### Option C: Rebuild Downstream Layers (Best for Medallion)

For silver and gold tables that are derived from bronze via transformation notebooks:
- Only sync **bronze** tables from Blue to Green
- **Re-run the transformation notebooks** against Green to rebuild silver/gold
- This ensures transformations are validated with the new schema structure

> **Recommendation:** Use Option C for silver/gold layers (rebuild deterministically) and Option A or B for bronze/raw tables (direct sync).

### 5.4 — Files Section Migration

The `Files/` section of the lakehouse (unstructured data) must also be migrated:

```python
# Copy Files/ from Blue to Green using mssparkutils or AzCopy
import subprocess

source = "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<blue_lakehouse_id>/Files/"
target = "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<green_lakehouse_id>/Files/"

mssparkutils.fs.cp(source, target, recurse=True)
```

---

## Phase 6: Artifact Migration — Notebooks

**Goal:** Update all notebooks to reference the Green lakehouse with schema-qualified table names.

### 6.1 — Pattern-by-Pattern Migration

#### Three-Part SQL References

```python
# BEFORE
spark.sql("SELECT * FROM Bronze.Address")
df.write.saveAsTable("Silver.fact_sale")

# AFTER (four-part naming with schema)
spark.sql("SELECT * FROM my_workspace.green_lakehouse.bronze_sales.Address")
df.write.saveAsTable("gold_reporting.fact_sale")

# AFTER (if default lakehouse is set to Green)
spark.sql("SELECT * FROM bronze_sales.Address")
df.write.saveAsTable("gold_reporting.fact_sale")
```

#### Path-Based References

```python
# BEFORE
df.write.format("delta").save("Tables/fact_sale")
df = spark.read.format("parquet").load("Files/raw/customers.csv")

# AFTER (schema-aware path — note: Tables now include schema)
df.write.mode("overwrite").saveAsTable("gold_reporting.fact_sale")
# Files/ path remains the same if using the same lakehouse
df = spark.read.format("parquet").load("Files/raw/customers.csv")
```

> **Key insight:** Prefer `saveAsTable("schema.table")` over path-based `save("Tables/...")` for schema-enabled lakehouses. The Spark catalog handles schema routing.

#### ABFSS Absolute Paths

```python
# BEFORE
path = "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<blue_id>/Tables/fact_sale"

# AFTER (update lakehouse ID + add schema path segment)
path = "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<green_id>/Tables/gold_reporting/fact_sale"

# BETTER: Parameterize
lakehouse_id = spark.conf.get("spark.custom.lakehouse_id")
schema = "gold_reporting"
table = "fact_sale"
path = f"abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema}/{table}"
```

### 6.2 — Notebook METADATA Block Update

Every notebook has a `# META` block with the default lakehouse binding:

```python
# BEFORE
# META {
#   "dependencies": {
#     "lakehouse": {
#       "default_lakehouse": "7d0dfe91-...",          # Blue lakehouse GUID
#       "default_lakehouse_name": "sales_lakehouse",
#       "default_lakehouse_workspace_id": "a8cbda3d-..."
#     }
#   }
# }

# AFTER
# META {
#   "dependencies": {
#     "lakehouse": {
#       "default_lakehouse": "NEW-GREEN-GUID-...",     # Green lakehouse GUID
#       "default_lakehouse_name": "sales_lakehouse_v2",
#       "default_lakehouse_workspace_id": "a8cbda3d-..."
#     }
#   }
# }
```

**CI/CD approach:** Use `parameter.yml` in the `fabric-cicd` package to replace lakehouse GUIDs at deployment time:

```yaml
# parameter.yml addition
find_replace:
  - find: "7d0dfe91-old-blue-guid"
    replace:
      DEV: "new-green-guid-dev"
      QA: "new-green-guid-qa"
      PROD: "new-green-guid-prod"
```

### 6.3 — Notebook Migration Checklist

For each notebook:

- [ ] Update default lakehouse METADATA to Green
- [ ] Replace bare table names with `schema.table` or four-part names
- [ ] Replace `Tables/tablename` paths with `saveAsTable("schema.table")`
- [ ] Update any ABFSS paths with new lakehouse GUID
- [ ] Parameterize where possible (Variable Library, widgets)
- [ ] Test against Green lakehouse in dev environment
- [ ] Verify output data lands in correct schema

---

## Phase 7: Artifact Migration — Pipelines & Dataflows

**Goal:** Update pipeline definitions and dataflow destinations to reference the Green lakehouse.

### 7.1 — Pipeline Migration

Pipeline JSON definitions contain lakehouse references in several places:

#### LakehouseTable Source/Sink Activities

```json
// BEFORE
"typeProperties": {
  "table": "fact_sale"
},
"linkedService": {
  "name": "blue_lakehouse",
  "properties": {
    "typeProperties": {
      "artifactId": "OLD-BLUE-ARTIFACT-ID"
    }
  }
}

// AFTER — update artifact ID and add schema awareness
"typeProperties": {
  "table": "fact_sale",
  "schema": "gold_reporting"      // Add schema reference
},
"linkedService": {
  "name": "green_lakehouse",
  "properties": {
    "typeProperties": {
      "artifactId": "NEW-GREEN-ARTIFACT-ID"
    }
  }
}
```

> **Note:** Verify whether your version of Fabric pipeline JSON supports the `"schema"` property in `LakehouseTable` type. If not, the table must be referenced as `schema.table` in the table name field, or use a notebook activity instead.

#### Lookup and ForEach Activities

- Update any Lookup activities that read from lakehouse tables
- Update ForEach loops that iterate over lakehouse metadata
- Update variable assignments referencing lakehouse IDs

### 7.2 — Dataflow Gen2 Migration

Dataflow Gen2 destinations that target the lakehouse need updating:
- Update the destination lakehouse reference
- Update the target table name to include schema (`schema.table`)
- Reconfigure the destination settings in the Fabric portal

### 7.3 — Pipeline Migration Checklist

For each pipeline:

- [ ] Update all `artifactId` references to Green lakehouse
- [ ] Add schema references to LakehouseTable activities
- [ ] Update any Lookup/GetMetadata activities
- [ ] Test each activity individually (Copy, Lookup, ForEach)
- [ ] Test end-to-end pipeline execution in dev
- [ ] Verify data lands in correct schema in Green

---

## Phase 8: Artifact Migration — Semantic Models & Reports

**Goal:** Rebind semantic models to the Green lakehouse. This is the highest-risk artifact migration because it directly impacts business users.

### 8.1 — DirectLake Semantic Models

DirectLake models are tightly coupled to lakehouse Delta tables. Update the TMDL definition:

```tmdl
// BEFORE
partition fact_sale = entity
  mode: directLake
  source
    entityName: fact_sale
    schemaName: dbo
    expressionSource: DatabaseQuery

// AFTER
partition fact_sale = entity
  mode: directLake
  source
    entityName: fact_sale
    schemaName: gold_reporting          // Updated to new schema
    expressionSource: DatabaseQuery     // May need to update expression source name
```

Also update the model's expression source to point to the Green lakehouse:

```tmdl
// Update the expression that points to the lakehouse
expression DatabaseQuery =
  let
    database = Sql.Database("GREEN_SQL_ENDPOINT_URL", "GREEN_LAKEHOUSE_NAME")
  in
    database
```

#### DirectLake Rebinding Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Semantic model owner lacks Green access | Framing fails silently | Pre-grant owner permissions in Phase 2 |
| SSO users lose access | Reports error for end users | Validate OneLake security roles |
| Cold cache after cutover | Temporary performance regression | Pre-warm by querying key tables |
| Object mapping breaks if names changed | Columns/measures disappear | Keep table names identical where possible |
| SQL endpoint fallback behavior changes | DirectQuery fallback may fail | Test with Direct Lake Behavior = "Automatic" and "Direct Lake Only" |

### 8.2 — Import Semantic Models

Update Power Query M expressions to reference the new lakehouse:

```m
// BEFORE
let Source = Lakehouse.Contents(null, "blue_lakehouse_id") in ...

// AFTER
let Source = Lakehouse.Contents(null, "green_lakehouse_id") in ...
```

Then trigger a full refresh to reload data from Green.

### 8.3 — DirectQuery Semantic Models

Update the SQL endpoint connection. The SQL analytics endpoint URL changes when the lakehouse changes:

- Old endpoint: `<blue_lakehouse_guid>.datawarehouse.fabric.microsoft.com`
- New endpoint: `<green_lakehouse_guid>.datawarehouse.fabric.microsoft.com`

### 8.4 — Reports, Dashboards, Apps & Subscriptions

Reports are bound to semantic models, not directly to lakehouses. Once the semantic model is updated, reports should work. However:

- **Validate every report page** — field mappings may break if table/column names changed
- **Check subscriptions** — email subscriptions tied to reports will continue working if the report still functions
- **Check apps** — Power BI apps that include affected reports need re-publishing
- **Check dashboards** — pinned tiles from affected reports need verification

### 8.5 — Semantic Model Migration Checklist

For each semantic model:

- [ ] Identify model type (DirectLake, Import, DirectQuery)
- [ ] Update lakehouse/SQL endpoint reference
- [ ] Update schema references in entity bindings
- [ ] Validate model owner has Green lakehouse access
- [ ] Deploy updated model to dev environment
- [ ] Trigger refresh (Import) or framing (DirectLake)
- [ ] Open every dependent report and verify rendering
- [ ] Check all visuals load without errors

---

## Phase 9: Artifact Migration — Shortcuts, SQL Endpoints & External Consumers

### 9.1 — OneLake Shortcuts

#### Inbound Shortcuts (pointing INTO the source lakehouse from other lakehouses)

These shortcuts reference the Blue lakehouse by artifact ID and path. They must be recreated to point to the Green lakehouse:

```python
# Discovery: Find all shortcuts pointing to the Blue lakehouse
# Check shortcut metadata tables or use the OneLake Shortcuts REST API
```

#### Outbound Shortcuts (FROM the source lakehouse to external storage)

If you use shortcuts from the Blue lakehouse to ADLS Gen2 or other lakehouses, you can recreate these as **schema shortcuts** in the Green lakehouse — one of the key benefits of migrating.

#### Shortcut Migration Steps

1. Inventory all shortcuts using the REST API or metadata tables
2. Create corresponding shortcuts in Green lakehouse
3. Validate that shortcut tables are accessible and data is current
4. Update any notebook code that references shortcuts by path

### 9.2 — SQL Analytics Endpoint

The Green lakehouse will have its own SQL analytics endpoint with a new connection string. Update:

- External tools (Azure Data Studio, SSMS, etc.)
- DirectQuery semantic models
- Any ODBC/JDBC connections
- Stored procedures or views defined on the SQL endpoint (these do not migrate automatically)

### 9.3 — External Consumers

For any external applications or tools that access the lakehouse via ABFSS URLs or OneLake APIs:

- Update connection strings / URIs
- Update authentication tokens if the lakehouse is in a different workspace
- Test data access from external tools before cutover

---

## Phase 10: CI/CD Pipeline Updates

**Goal:** Update the `fabric-cicd` deployment configuration so automated deployments target the Green lakehouse.

### 10.1 — Configuration File Updates

#### `attached_config/target_deployment.yml`

If the lakehouse item type is in scope, verify the workspace IDs are correct. If the Green lakehouse is in the same workspace, no change needed here. If in a new workspace, update the workspace ID.

#### `fabric_items/parameter.yml`

Add parameter replacements for the new lakehouse:

```yaml
# New entries for Green lakehouse
find_replace:
  # Lakehouse artifact ID replacement
  - find: "OLD-BLUE-LAKEHOUSE-GUID"
    replace:
      DEV: "GREEN-LAKEHOUSE-GUID-DEV"
      QA: "GREEN-LAKEHOUSE-GUID-QA"
      PROD: "GREEN-LAKEHOUSE-GUID-PROD"
  
  # Lakehouse name replacement (if renamed)
  - find: "blue_lakehouse_name"
    replace:
      DEV: "green_lakehouse_name"
      QA: "green_lakehouse_name"
      PROD: "green_lakehouse_name"
```

### 10.2 — Notebook Metadata Parameterization

Ensure the `parameter.yml` covers notebook METADATA blocks so that `default_lakehouse` GUIDs are replaced per-environment during deployment.

### 10.3 — Pipeline Artifact ID Parameterization

Ensure pipeline JSON `artifactId` references are parameterized so they point to the correct lakehouse per environment.

### 10.4 — CI/CD Testing

1. Deploy to DEV workspace using updated configuration
2. Run all pipelines in DEV
3. Validate data flows to Green lakehouse in DEV
4. Promote to QA
5. Full regression test in QA
6. Only then promote to PROD

---

## Phase 11: Validation & Rehearsal

**Goal:** Prove that Green is a complete and correct replica of Blue before any cutover happens in production.

### 11.1 — Data Validation Framework

#### Row Count Comparison

```python
# Compare row counts between Blue and Green for all tables
discrepancies = []
for t in tables_to_migrate:
    blue_count = spark.table(f"blue_lakehouse.{t['source']}").count()
    green_count = spark.table(f"{t['target_schema']}.{t['target_table']}").count()
    
    if blue_count != green_count:
        discrepancies.append({
            "table": t['source'],
            "blue": blue_count,
            "green": green_count,
            "diff": blue_count - green_count
        })

if discrepancies:
    display(spark.createDataFrame(discrepancies))
else:
    print("✅ All row counts match!")
```

#### Hash-Based Integrity Check

```python
from pyspark.sql.functions import md5, concat_ws, col

def table_hash(df, pk_col):
    """Generate a deterministic hash for data comparison."""
    cols = sorted(df.columns)
    return df.select(
        col(pk_col),
        md5(concat_ws("||", *[col(c).cast("string") for c in cols])).alias("row_hash")
    )

blue_hash = table_hash(spark.table("blue_lakehouse.fact_sale"), "sale_id")
green_hash = table_hash(spark.table("gold_reporting.fact_sale"), "sale_id")

mismatches = blue_hash.join(green_hash, "sale_id", "full_outer") \
    .filter("blue.row_hash != green.row_hash OR blue.row_hash IS NULL OR green.row_hash IS NULL")

print(f"Mismatched rows: {mismatches.count()}")
```

### 11.2 — Great Expectations Validation

Leverage the existing `GreatExpectations.Environment` and `ValidateDataQuality.Notebook` patterns:

```python
import great_expectations as gx

context = gx.get_context()

# Validate Green lakehouse tables against existing expectations
for table in green_tables:
    batch = context.get_batch(
        datasource_name="green_lakehouse",
        data_connector_name="default",
        data_asset_name=table
    )
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch]
    )
    assert results["success"], f"Validation failed for {table}"
```

### 11.3 — End-to-End Pipeline Smoke Test

Run every pipeline end-to-end in the dev/test environment against Green:

1. Trigger each pipeline manually
2. Verify completion without errors
3. Check that output tables exist in the correct schemas
4. Verify row counts and data freshness

### 11.4 — Semantic Model & Report Validation

- Open every report bound to migrated semantic models
- Verify all visuals load
- Spot-check data values against known-good data
- Test with multiple user identities (to validate RLS/security)

### 11.5 — Rehearsal in Dev/Test

Perform a **full dress rehearsal** of the cutover process in the dev/test environment:

1. Execute every phase of the cutover runbook (Phase 12)
2. Time each step
3. Document issues encountered
4. Run rollback procedure to validate it works
5. Repeat until the team is confident

---

## Phase 12: Cutover

**Goal:** Redirect all production workloads from Blue to Green. This is an **artifact-by-artifact coordinated switchover**, not a single switch.

### 12.1 — Pre-Cutover Checklist

- [ ] All data in Green matches Blue (validation passed)
- [ ] All notebooks updated and tested in dev/test
- [ ] All pipelines updated and tested in dev/test
- [ ] All semantic models updated and tested in dev/test
- [ ] All shortcuts recreated and validated
- [ ] Security permissions configured on Green
- [ ] CI/CD pipelines updated and tested
- [ ] Capacity has sufficient headroom
- [ ] Rollback plan reviewed and rehearsed
- [ ] Stakeholders notified of cutover window
- [ ] On-call team identified

### 12.2 — Cutover Sequence

Execute in this order to minimize disruption:

```
Step 1: FREEZE WRITES
  → Pause all scheduled pipelines that write to Blue
  → Wait for any in-flight jobs to complete

Step 2: FINAL SYNC
  → Run one last incremental sync from Blue to Green
  → Verify row counts match exactly

Step 3: REDIRECT WRITES
  → Deploy updated notebooks (now writing to Green) via CI/CD
  → Deploy updated pipelines (now targeting Green) via CI/CD
  → This is the point of no return for data writes

Step 4: REDIRECT READS
  → Update and deploy semantic models pointing to Green
  → Update shortcuts pointing to Green
  → Update SQL endpoint connections

Step 5: VALIDATE
  → Run smoke tests on all pipelines
  → Open all critical reports
  → Check monitoring for errors

Step 6: ANNOUNCE
  → Notify stakeholders that cutover is complete
  → Begin monitoring grace period
```

### 12.3 — Cutover Timing Recommendations

- Execute during **lowest traffic window** (weekends or off-hours)
- Allow **2-4 hours** for the full sequence (based on rehearsal timing)
- Have the **rollback plan ready** with designated decision maker

---

## Phase 13: Rollback Plan

**Goal:** Define clear rollback procedures for different failure scenarios. Rollback complexity depends on whether Green has accepted any writes.

### 13.1 — Rollback Classes

| Class | Condition | Rollback Procedure | Data Risk |
|---|---|---|---|
| **Config-only** | Green hasn't accepted writes yet (Steps 1-2) | Revert CI/CD deployment; resume Blue pipelines | ✅ None |
| **Partial cutover** | Some artifacts switched, Green has partial writes (Step 3 in progress) | Revert switched artifacts; reverse-sync Green→Blue for any diverged tables | ⚠️ Moderate — need reverse sync |
| **Full cutover** | All artifacts switched, Green is authoritative (Steps 4+) | Full reverse migration (significant effort) | ❌ High — Green has new data Blue doesn't |

### 13.2 — Config-Only Rollback (Safe)

If issues are detected before writes hit Green:

1. Revert CI/CD deployment to previous version (redeploy Blue-targeting artifacts)
2. Resume all paused pipelines
3. Verify Blue is receiving writes normally
4. Investigate and fix the issue before re-attempting

### 13.3 — Post-Write Rollback (Complex)

If Green has accepted writes and you need to roll back:

1. **Freeze writes** to Green immediately
2. **Identify diverged tables** — which tables have data in Green that Blue doesn't?
3. **Reverse sync** diverged data from Green → Blue
4. **Redeploy** Blue-targeting artifacts via CI/CD
5. **Resume** Blue pipelines
6. **Validate** data integrity in Blue

### 13.4 — Rollback Decision Framework

| Signal | Action |
|---|---|
| One report visual broken | Fix forward — update the visual binding |
| One pipeline failing | Fix forward — debug and patch |
| Multiple pipelines failing | Assess — is it a systemic issue or independent bugs? |
| Data integrity issues (wrong counts, missing data) | **Rollback** unless root cause is immediately clear |
| Security/access failures at scale | **Rollback** and fix security config |
| Capacity overload (throttling) | Pause non-critical workloads; consider partial rollback |

### 13.5 — Grace Period

After cutover, keep Blue lakehouse available (read-only) for a defined grace period:

- **Recommended:** 14-30 days
- During this period, Blue serves as a backup reference
- After the grace period, decommission Blue:
  - Remove scheduled jobs
  - Archive the lakehouse (or delete if storage cost is a concern)
  - Update documentation

---

## Phase 14: Post-Migration Governance

**Goal:** Establish ongoing governance practices now that schemas are in place.

### 14.1 — Schema Naming Convention

Enforce the hybrid medallion + domain convention across the organization:

```
<layer>_<domain>    — for data tables
  bronze_sales, silver_finance, gold_analytics

staging             — for temporary/intermediate tables
metadata            — for pipeline metadata and audit logs
dbo                 — default schema (use sparingly)
```

Publish this as a team standard and enforce via code review.

### 14.2 — Access Control Standards

| Role | Default Access | Escalation |
|---|---|---|
| Data Engineers | Read/Write to their domain schemas | Request access to other domains via team lead |
| Data Analysts | Read-only to gold schemas | Request access via data governance team |
| Data Scientists | Read-only to silver/gold schemas | Write access to `staging` for experiments |
| Power BI Authors | Read-only to gold schemas (via semantic model) | DirectLake requires OneLake read access |
| Service Principals | Scoped to specific schemas per automation | Audit quarterly |

### 14.3 — Four-Part Naming Standard

Require four-part naming (`workspace.lakehouse.schema.table`) in all new notebooks and cross-workspace queries. This future-proofs the codebase for subsequent migrations or lakehouse reorganizations.

### 14.4 — Monitoring

- Set up capacity alerts for CU utilization spikes
- Monitor DirectLake framing success/failure rates
- Track schema-level storage growth
- Audit access logs for unauthorized schema access

---

## Fabric-Specific Gotchas

These are the non-obvious behaviors that can cause migration failures if not anticipated:

| Gotcha | Description | Mitigation |
|---|---|---|
| **No in-place schema enablement** | You cannot toggle schemas on an existing lakehouse | Blue/green migration is the only path |
| **`dbo` is the stealth default** | Bare table names resolve to `dbo` schema. This can mask migration bugs where code *appears* to work but lands in the wrong schema | Always use explicit schema names |
| **Notebook default lakehouse must be schema-enabled** | If the notebook's default lakehouse isn't schema-enabled, schema references won't work | Update METADATA blocks or remove default lakehouse |
| **Schema-enabled lakehouses can't be workspace-shared** | Unlike non-schema lakehouses, you can't share them via workspace sharing | Use shortcuts as a workaround |
| **DirectLake owner matters** | The semantic model owner (or fixed identity) must have OneLake access to the Green lakehouse. If not, framing fails silently | Pre-validate owner permissions |
| **Shortcuts are path-fragile** | If a target table moves to a different schema (different path), shortcuts break | Recreate shortcuts after schema changes |
| **Moving tables between schemas breaks references** | Using drag-and-drop in Lakehouse Explorer to move tables doesn't update references | Always update consuming code first |
| **SQL endpoint is different per lakehouse** | The SQL analytics endpoint URL changes when you switch to a new lakehouse | Update all DirectQuery and external SQL consumers |
| **Schema names: alphanumeric + underscore only** | No hyphens, spaces, or special characters | Validate names before creating schemas |

---

## Decision Register

Track key decisions made during migration planning:

| # | Decision | Options Considered | Choice | Rationale | Date |
|---|---|---|---|---|---|
| D1 | Schema design pattern | Medallion only, Domain only, Hybrid | Hybrid (medallion + domain) | Balances data engineering workflow with business organization | |
| D2 | Migration strategy | In-place (not possible), Side-by-side, Gradual | Blue/Green (zero-downtime) | Organization cannot tolerate downtime | |
| D3 | Write-truth during transition | Blue only, Dual-write, Green only | Blue remains write-truth | Simplifies rollback and avoids data divergence | |
| D4 | Naming standard | Two-part, Three-part, Four-part | Four-part naming | Future-proofs for cross-workspace queries | |
| D5 | Sync strategy | Full copy, Watermark, CDF, Rebuild | Rebuild silver/gold; sync bronze | Validates transformations in new schema context | |
| D6 | Rollback grace period | 7 days, 14 days, 30 days | TBD | Balance between safety and storage cost | |

---

## Appendix: Schema Naming Convention

### Allowed Characters
- Letters (a-z, A-Z)
- Numbers (0-9)
- Underscores (_)

### Prohibited
- Hyphens (-)
- Spaces
- Special characters
- SQL reserved words (avoid: `default`, `master`, `model`, `temp`)

### Convention Template
```
<layer>_<domain>[_<subdomain>]

Examples:
  bronze_sales
  bronze_sales_pos           (subdomain: point-of-sale)
  silver_finance
  silver_finance_gl          (subdomain: general ledger)
  gold_analytics
  gold_reporting_executive
```

---

## Appendix: Discovery Queries

### List All Tables in Source Lakehouse
```python
tables = spark.catalog.listTables("source_lakehouse")
for t in tables:
    print(f"{t.name} | {t.tableType} | {t.isTemporary}")
```

### Find All Notebook References to a Lakehouse
```bash
# Search for hardcoded lakehouse IDs
grep -rn "OLD_LAKEHOUSE_GUID" fabric_items/**/*.py

# Search for table references
grep -rn "spark.sql\|spark.table\|saveAsTable" fabric_items/**/*.py

# Search for ABFSS paths
grep -rn "abfss://" fabric_items/**/*.py
```

### Find Pipeline Artifact References
```bash
grep -rn "artifactId" fabric_items/**/*.json | grep -i "lakehouse"
```

### Profile Table Sizes
```python
from pyspark.sql.functions import count

tables = spark.catalog.listTables("source_lakehouse")
for t in tables:
    try:
        row_count = spark.table(f"source_lakehouse.{t.name}").count()
        detail = spark.sql(f"DESCRIBE DETAIL source_lakehouse.{t.name}").collect()[0]
        print(f"{t.name}: {row_count:,} rows, {detail.sizeInBytes / 1024 / 1024:.1f} MB")
    except Exception as e:
        print(f"{t.name}: ERROR - {e}")
```

---

## Appendix: Validation Checklist

### Pre-Cutover Validation
- [ ] Row counts match for 100% of tables
- [ ] Hash-based integrity passes for critical tables
- [ ] Great Expectations suite passes on Green
- [ ] All pipelines complete successfully in dev/test
- [ ] All semantic models refresh/frame successfully
- [ ] All reports render correctly
- [ ] Security permissions validated with test users
- [ ] CI/CD deploys successfully to dev/test
- [ ] Rollback procedure tested in dev/test

### Post-Cutover Validation (Day 1)
- [ ] All scheduled pipelines running on Green
- [ ] No errors in monitoring hub
- [ ] Semantic model refresh succeeds
- [ ] Reports accessible to end users
- [ ] No unauthorized access attempts logged
- [ ] Capacity utilization within normal bounds

### Post-Cutover Validation (Week 1)
- [ ] Data freshness meets SLA
- [ ] No user-reported issues
- [ ] All downstream consumers functioning
- [ ] Blue lakehouse still accessible (read-only) as backup
- [ ] Documentation updated

---

*Last updated: April 2026*
*Playbook version: 1.0*
