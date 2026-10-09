# Migrating from Fabric Data Warehouse to Fabric Lakehouse

A vendor-neutral, general-purpose guide for teams evaluating or executing a move of an
analytics workload from a **Microsoft Fabric Warehouse** to a **Microsoft Fabric Lakehouse**.

> This guide contains **no customer-specific information**. All examples use generic
> schema and table names (`sales`, `dim_customer`, `fact_order`, etc.).

---

## Start here

**The single most important thing to understand before you begin:**

A Fabric Warehouse is a **read/write** T-SQL engine. A Fabric Lakehouse exposes a
**read-only** SQL analytics endpoint. It supports full DQL (`SELECT`), **no DML**
(`INSERT`/`UPDATE`/`DELETE`/`MERGE`), and only limited DDL (views, functions, stored
procedures — but **not** tables). All writes must move to Spark, pipelines, dataflows,
or shortcuts.

If your transformation logic lives in T-SQL stored procedures today, **that logic does
not carry over**. It must be rewritten. Everything else in this guide is secondary to
that fact.

---

## Documents

| # | Document | What it answers |
|---|---|---|
| 1 | [01-decision-framework.md](01-decision-framework.md) | *Should* we migrate? Capability comparison, drivers, anti-drivers, and the "run both" option. |
| 2 | [02-capability-gap-analysis.md](02-capability-gap-analysis.md) | What specifically breaks or changes. Feature-by-feature gap table. |
| 3 | [03-migration-playbook.md](03-migration-playbook.md) | The phased execution plan, from assessment to cutover. |
| 4 | [04-tsql-to-spark-patterns.md](04-tsql-to-spark-patterns.md) | Code translation patterns: stored procs, MERGE, SCD2, identity columns, temp tables. |
| 5 | [05-security-mapping.md](05-security-mapping.md) | Mapping Warehouse SQL security (RLS/CLS/DDM/OLS) to Lakehouse equivalents. |
| 6 | [06-operations-and-performance.md](06-operations-and-performance.md) | Table maintenance, metadata sync, Direct Lake, capacity and CU implications. |
| 7 | [07-validation-and-cutover.md](07-validation-and-cutover.md) | Parity testing, cutover sequencing, rollback. |
| 8 | [08-reference-architecture.md](08-reference-architecture.md) | Target medallion patterns, including the hybrid Lakehouse + Warehouse pattern. |
| — | [references.md](references.md) | Source links to Microsoft Learn. |

### Supporting assets

| Path | Purpose |
|---|---|
| [`assessment/warehouse_inventory.sql`](assessment/warehouse_inventory.sql) | T-SQL script that inventories every object in a source Warehouse and flags migration blockers. |
| [`assessment/parity_validation.py`](assessment/parity_validation.py) | PySpark notebook script that compares row counts, aggregates, and schemas between source Warehouse and target Lakehouse. |
| [`assessment/migration_scorecard.md`](assessment/migration_scorecard.md) | Fill-in scorecard to make the stay/move decision defensible. |

---

## Five-minute summary

### What stays the same

- **Storage format.** Both store data in open **Delta Lake** format in **OneLake**.
  No proprietary lock-in either way, and no format conversion is required.
- **Query engine for SQL reads.** The Lakehouse SQL analytics endpoint runs on the
  *same* engine as Fabric Data Warehouse. Read performance characteristics are similar.
- **Power BI connectivity.** Direct Lake, DirectQuery, and Import all remain available.
- **Cross-item queries.** Three-part-name queries across lakehouses and warehouses
  continue to work.

### What changes

| Dimension | Warehouse | Lakehouse |
|---|---|---|
| Write path | T-SQL (`INSERT`, `COPY INTO`, `CTAS`), pipelines, dataflows | Spark, pipelines, dataflows, shortcuts |
| T-SQL surface | Full DQL + DML + DDL | Full DQL, **no DML**, limited DDL |
| Multi-table transactions | Supported | **Not supported** |
| Layout maintenance | System-managed | **User-managed** (`OPTIMIZE`, `VACUUM`) |
| Primary skill set | SQL developers | Data engineers (PySpark / Spark SQL) |
| Unstructured / semi-structured data | Not supported | First-class (`Files/` section) |
| Shortcuts as an ingestion path | Limited | First-class |
| Warehouse snapshots, zero-copy clone | Supported | Not available |

### When migration is usually the right call

- Transformation logic is already (or is moving to) Spark/Python.
- You need to land unstructured or semi-structured data alongside tables.
- You want zero-copy access to external lakes via shortcuts.
- You need Delta tables readable by external engines (other Spark platforms,
  Iceberg/Delta readers) without an intermediary.
- Your team's centre of gravity is data engineering, not SQL development.

### When migration is usually the wrong call

- Your ETL is a large estate of T-SQL stored procedures and your team is SQL-first.
- You depend on multi-table ACID transactions.
- You rely on Warehouse-only features: `IDENTITY` columns, zero-copy clone,
  warehouse snapshots, system-managed layout.
- The driver is "lakehouse is more modern" rather than a concrete requirement.

> **"Stay on Warehouse"** and **"run both, each for what it is good at"** are valid,
> defensible outcomes. Treat them as first-class options, not failures.

---

## How to use this guide

1. Run [`assessment/warehouse_inventory.sql`](assessment/warehouse_inventory.sql)
   against the source Warehouse.
2. Complete [`assessment/migration_scorecard.md`](assessment/migration_scorecard.md).
3. If the scorecard says *migrate*, work through
   [03-migration-playbook.md](03-migration-playbook.md) phase by phase.
4. If it says *stay* or *hybrid*, use
   [08-reference-architecture.md](08-reference-architecture.md) to design the split.

---

## Status and versioning

Fabric evolves quickly. Every capability claim in this guide is traceable to a link in
[references.md](references.md). Before acting on any statement, re-check the linked
page — particularly anything labelled **preview**.

Last reviewed against Microsoft Learn: **October 2026**.
