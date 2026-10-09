# 01 — Decision Framework

Before planning a migration, establish that one is warranted. This document gives you a
structured way to reach a defensible answer.

---

## 1. Establish the actual driver

Do not start with the technology. Start with the question: **what problem are we
solving?** In practice, stated drivers fall into a small number of buckets, and only
some of them are solved by moving to a Lakehouse.

| Stated driver | Is Lakehouse the fix? | Notes |
|---|---|---|
| "We want to use Spark / Python for transformations" | **Yes** | This is the strongest driver. Warehouse has no Spark surface. |
| "We need to store unstructured or semi-structured data" | **Yes** | Lakehouse `Files/` section handles this; Warehouse does not. |
| "We want external engines to read our tables directly" | **Partly** | Warehouse *also* stores Delta in OneLake and can be a shortcut source. Test before assuming. |
| "We want zero-copy access to an external data lake" | **Yes** | Shortcuts are first-class in Lakehouse, limited in Warehouse. |
| "We want lower cost" | **Unproven** | Both consume Fabric CUs. Cost depends on workload shape, not item type. Measure first. |
| "Lakehouse is more modern / is the strategic direction" | **No** | Both are strategic, first-party, and actively invested in. |
| "We are consolidating onto another Spark platform" | **Maybe** | Validate the interop requirement concretely — both items write open Delta to OneLake. |
| "Our queries are slow" | **No** | Both share the same SQL engine. Investigate layout, model, and capacity first. |

> **Rule:** if you cannot name the driver in one sentence that references a capability
> the Lakehouse has and the Warehouse does not, stop and re-scope.

---

## 2. Capability comparison

### 2.1 Core warehousing capabilities

| Capability | Warehouse | Lakehouse (SQL analytics endpoint) |
|---|---|---|
| Primary capability | ACID-compliant full data warehousing with multi-table transactions in T-SQL | Read-only, system-generated T-SQL endpoint for querying and serving |
| Developer profile | SQL developers, citizen developers | Data engineers, SQL developers |
| Data loading | T-SQL (`COPY INTO`, `INSERT`, `CTAS`), pipelines, dataflows | Apache Spark, pipelines, dataflows, shortcuts |
| Delta table support | **Reads and writes** Delta tables | **Reads** Delta tables |
| Storage layer | Open Delta format in OneLake | Open Delta format in OneLake |
| T-SQL capabilities | Full DQL, DML, and DDL with full transaction support | Full DQL, **no DML**, limited DDL (views, table-valued functions, stored procedures, functions) |
| Development surface | Warehouse editor; read/write support for first- and third-party tooling | Endpoint editor; **limited** T-SQL support for first- and third-party tooling |

### 2.2 Analytical data store comparison

| Capability | Warehouse | Lakehouse |
|---|---|---|
| Multi-table transaction support | ✅ | ❌ |
| Single-table transaction support | ✅ | ✅ |
| Optimized for large scans and aggregations | ✅ | ✅ |
| Optimized for selective lookups | ❌ | ❌ |
| Object-level security | ✅ | ✅ |
| Column-level security | ✅ | ✅ (see [05-security-mapping.md](05-security-mapping.md)) |
| Row-level security | ✅ | ✅ (see [05-security-mapping.md](05-security-mapping.md)) |
| T-SQL commands as an ingestion tool | ✅ | ❌ |
| Shortcuts as an ingestion tool | ⚠️ limited | ✅ |
| Eventstreams as an ingestion tool | ❌ | ✅ |
| Spark connectors | ⚠️ limited | ✅ |
| Parsing of semi-structured data | ⚠️ limited | ✅ |
| Parsing of unstructured data | ❌ | ✅ |
| T-SQL surface area | Broad | **Limited** |
| Python support | ❌ | ✅ |
| Spark support (PySpark, Spark SQL, Scala, R) | ❌ | ✅ |
| Transformation extensibility | Moderate | **Very high** |
| Efficiency of updates and deletes | Moderate | Moderate |
| Compute configuration control | Moderate | **High** |
| Admin skill needed to tune compute | **Low** | **Moderate–high** |

The last two rows matter more than teams expect. Moving to a Lakehouse trades
*autonomous* workload management for *configurable* workload management. That is a
benefit if you want the control and a cost if you do not.

---

## 3. The decision tree

Work through these in order. The first decisive answer wins.

1. **How do you want to develop?**
   - Apache Spark (Python, Scala, Spark SQL, R) → **Lakehouse**
   - T-SQL → **Warehouse**

2. **Do you need multi-table transactions?**
   - Yes → **Warehouse**
   - No → continue

3. **What type of data are you analysing?**
   - Unstructured and structured, or unsure → **Lakehouse**
   - Structured only → **Warehouse**

4. **Do you depend on any Warehouse-only feature?**
   (`IDENTITY` columns, zero-copy clone, warehouse snapshots, `COPY INTO`,
   system-managed table layout, multi-statement transactions)
   - Yes → **Warehouse**, or scope the replacement explicitly

5. **Do you need unstructured file landing, shortcuts to external lakes, or Spark
   connectors as a primary ingestion path?**
   - Yes → **Lakehouse**

---

## 4. The option everyone forgets: run both

Warehouse and Lakehouse share the same OneLake storage and support cross-item queries
with zero duplication. A very common and healthy target state is:

```
Lakehouse (Bronze)  →  Lakehouse (Silver)  →  Warehouse (Gold)
   Spark ingest          Spark transform        T-SQL serving + semantic layer
```

or the inverse, if the serving layer is Spark-driven:

```
Lakehouse (Bronze/Silver/Gold)  →  Warehouse (curated marts for SQL-first consumers)
```

Choose a Warehouse for governed, high-performance, T-SQL-centric workloads and a
Lakehouse for big-data processing, exploratory analytics, and varied data formats.
Many organizations benefit from Lakehouses for ingestion and transformation, and
Warehouses for refined analytics and reporting.

See [08-reference-architecture.md](08-reference-architecture.md) for concrete layouts.

---

## 5. Cost: what to actually measure

Neither item is inherently cheaper. Before claiming a cost benefit, baseline:

| Metric | Where to get it |
|---|---|
| CU consumption by item, by operation class | Fabric Capacity Metrics app |
| Peak concurrent query CU | Capacity Metrics, interactive operations |
| Background CU for ETL windows | Capacity Metrics, background operations |
| OneLake storage GB by item | Capacity Metrics / OneLake storage report |
| Time-travel and unreferenced file overhead | `DESCRIBE HISTORY`, OneLake storage growth vs. active table size |

Then model the post-migration profile. Two effects commonly surprise teams:

- **Spark cluster start-up and session overhead** becomes a new, recurring CU line
  that did not exist with Warehouse's autonomous compute.
- **Maintenance jobs (`OPTIMIZE`, `VACUUM`) become your cost**, because layout
  management shifts from system-managed to user-managed.

---

## 6. Scoring the decision

Use [`assessment/migration_scorecard.md`](assessment/migration_scorecard.md). It
converts the above into a weighted score across six dimensions:

1. Transformation logic portability
2. Transactional requirements
3. Security parity requirements
4. Consumption-layer impact (Power BI, external tools)
5. Team skills and operating model
6. Interoperability requirement strength

A score is not a decision, but it makes the trade-offs explicit and reviewable.

---

## 7. Output of this phase

A one-page decision record containing:

- The named driver, in one sentence.
- The option chosen: **migrate**, **stay**, or **hybrid**.
- The top three risks accepted.
- The specific capabilities being given up, and their replacements.
- The measurable success criteria for the migration (not "it works" — e.g. "gold layer
  refresh completes in under 45 minutes at p95, with zero row-count variance").

Do not proceed to [03-migration-playbook.md](03-migration-playbook.md) without it.
