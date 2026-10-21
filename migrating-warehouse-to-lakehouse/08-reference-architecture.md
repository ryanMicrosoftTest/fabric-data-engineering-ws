# 08 — Reference Architectures

Four target patterns, with the conditions under which each is the right answer. All
names are generic.

---

## Pattern A — Full Lakehouse medallion

**Everything in lakehouses; the SQL analytics endpoint serves T-SQL consumers.**

```
 Sources ──► Lakehouse: bronze ──► Lakehouse: silver ──► Lakehouse: gold
             (raw, as-landed)      (conformed, typed)     (business-ready)
                                                               │
                                        ┌──────────────────────┼──────────────────────┐
                                        ▼                      ▼                      ▼
                              Direct Lake semantic     SQL analytics endpoint   External engines
                                     model              (read-only T-SQL)       (Delta over OneLake)
```

**Choose when**
- Transformation logic is Spark-native or expressible declaratively.
- No multi-table transaction requirement.
- Unstructured or semi-structured data is in scope.
- The team's centre of gravity is data engineering.

**Accept that**
- No T-SQL write path exists anywhere in the stack.
- You own all layout maintenance.
- SQL-layer security does not constrain Spark readers.

**Implementation notes**
- Use **schema-enabled lakehouses** so you keep three-part naming.
- Either one lakehouse per layer, or one lakehouse with `bronze` / `silver` / `gold`
  schemas. One lakehouse with schemas keeps cross-layer queries simple; separate
  lakehouses give cleaner security boundaries and independent lifecycle.
- Apply V-Order on gold only.
- Add a **Refresh SQL endpoint** step at the end of every gold-writing pipeline.

---

## Pattern B — Lakehouse for engineering, Warehouse for serving (hybrid)

**The most common healthy end state when the existing estate is T-SQL-heavy.**

```
 Sources ──► Lakehouse: bronze ──► Lakehouse: silver ──► Warehouse: gold
             Spark ingest          Spark transform       T-SQL marts, procs,
             shortcuts             MLVs                  multi-table transactions
                                                               │
                                        ┌──────────────────────┴──────────────────────┐
                                        ▼                                             ▼
                              Direct Lake semantic model                   SQL-first consumers,
                                                                           write-back applications
```

**Choose when**
- You want Spark for ingestion and heavy transformation, but the serving layer is
  T-SQL-centric.
- Gold-layer loads genuinely need multi-table atomicity.
- Warehouse-only features matter at the serving layer: `IDENTITY`, zero-copy clone,
  warehouse snapshots, system-managed layout.
- SQL-native security semantics (RLS, CLS, DDM via T-SQL) are a hard requirement for
  the served data.

**Accept that**
- Two items to operate, two security models to keep aligned.
- Data is materialized twice at the silver/gold boundary (or read cross-item).

**Implementation notes**
- Warehouse can read silver lakehouse tables via cross-item three-part names — no copy
  needed for read-only transforms.
- Keep the system of record for each table unambiguous.
- This pattern often **removes the need to migrate at all**: you add a Lakehouse
  upstream rather than replacing the Warehouse.

---

## Pattern C — Lakehouse with materialized lake views

**Declarative medallion, minimal hand-written Spark.**

```
 Sources ──► Lakehouse: bronze ──► MLV: silver_* ──► MLV: gold_*
             raw Delta tables      declarative SQL    declarative SQL
                                   + data quality     + data quality
                                   constraints        constraints
                                        │
                                        ▼
                         Fabric orchestrates dependency order,
                         refresh strategy (incremental / full / skip),
                         lineage, and quality enforcement
```

**Choose when**
- Your existing transformations are set-based SQL (which, coming from a Warehouse, they
  usually are).
- You want the smallest possible rewrite surface.
- Frequently accessed aggregations and complex recurring joins dominate.
- You want declarative data-quality constraints rather than hand-rolled checks.

**Do not choose when**
- Logic needs Python: ML inference, API calls, complex procedural processing.
- One-time or rarely accessed queries — materialization is wasted.
- Sub-second streaming freshness is required.

**Implementation notes**
- MLVs produce real Delta tables, so they are **Direct Lake friendly** — unlike plain
  SQL views, which force DirectQuery fallback (Direct Lake on SQL) or are unsupported
  (Direct Lake on OneLake).
- SQL authoring uses `CREATE MATERIALIZED LAKE VIEW`.
- PySpark authoring is in preview and currently **full refresh only** — confirm status
  before relying on incremental PySpark MLVs.
- Fabric tracks dependencies between MLVs and refreshes them in the correct order; you
  do not hand-build the DAG.

---

## Pattern D — Lakehouse as a federation layer

**Keep the Warehouse; add a Lakehouse purely for shortcut-based access to external data.**

```
 External lake / S3 / ADLS ──┐
                             ├──► Lakehouse (shortcuts only, no copies)
 Other Fabric workspaces ────┘            │
                                          ▼
                                   Warehouse (existing) reads via
                                   cross-item three-part names
```

**Choose when**
- The only real driver is "we need zero-copy access to data that lives elsewhere."
- The existing Warehouse is working well and the team is SQL-first.

**Accept that**
- Shortcut targets must be in a format the SQL endpoint can read (Delta under `Tables/`).
- In **delegated identity mode**, shortcuts to source tables that have *any* OneLake
  RLS/CLS/OLS are **blocked**. Plan the access mode accordingly.

This pattern delivers most of the usual "we need Lakehouse" benefit for a fraction of
the migration cost. Always evaluate it before committing to Pattern A.

---

## Choosing between the patterns

| Question | A | B | C | D |
|---|---|---|---|---|
| Is transformation logic T-SQL-heavy today? | ✗ | ✓ | ✓ | ✓ |
| Do you need multi-table transactions anywhere? | ✗ | ✓ | ✗ | ✓ |
| Is Spark/Python required for transformation? | ✓ | ✓ | ✗ | ✗ |
| Unstructured data in scope? | ✓ | ✓ | ✓ | ✗ |
| Zero-copy external lake access required? | ✓ | ✓ | ✓ | ✓ |
| Smallest rewrite effort? | ✗ | ~ | ✓ | ✓✓ |
| Fewest items to operate? | ✓ | ✗ | ✓ | ✗ |
| Warehouse-only features required at serving layer? | ✗ | ✓ | ✗ | ✓ |

---

## Cross-cutting design standards

Apply these regardless of pattern.

### Naming

```
<lakehouse>.<schema>.<table>

schemas:   bronze | silver | gold         (medallion)
       or  <domain>_bronze | ...          (domain-partitioned medallion)
tables:    dim_<entity> | fact_<process> | raw_<source>_<entity> | ctl_<purpose>
```

- Lower case, underscores, no spaces, no reserved words.
- Always use explicit three-part names in code. **Never rely on a default lakehouse.**

### Standard columns on every curated table

| Column | Type | Purpose |
|---|---|---|
| `batch_id` | STRING | Groups all rows written by one logical load |
| `load_ts` | TIMESTAMP | When the row was written |
| `source_system` | STRING | Provenance |
| `row_hash` | STRING / BIGINT | Change detection for merges and SCD |

These replace the guarantees that multi-table transactions used to provide.

### Batch control table

```sql
CREATE TABLE ctl.batch_log (
    batch_id      STRING,
    pipeline_name STRING,
    layer         STRING,
    started_at    TIMESTAMP,
    completed_at  TIMESTAMP,
    status        STRING,      -- RUNNING | SUCCEEDED | FAILED
    row_count     BIGINT,
    error_message STRING
) USING DELTA;
```

Downstream consumers read only data whose `batch_id` has `status = 'SUCCEEDED'`.

### Workspace topology

- Separate workspaces for dev, test, and prod, with deployment pipelines between them.
- **Avoid a very large number of lakehouses in a single workspace** — automatic metadata
  discovery is a single instance per workspace, and sync latency degrades.
- Keep the semantic model in the **same region** as the data source workspace; Direct
  Lake across regions is unsupported (workaround: a local lakehouse with shortcuts).

### Parameterization

No hard-coded workspace IDs, item IDs, or environment names in notebooks or pipelines.
Use a variable library or notebook parameters so the same artefact promotes unchanged
through dev → test → prod.

---

## Anti-patterns

| Anti-pattern | Why it hurts |
|---|---|
| Relying on the notebook's "default lakehouse" | Breaks silently on promotion between environments |
| SQL views feeding Direct Lake semantic models | Forces DirectQuery fallback, or is unsupported on Direct Lake on OneLake |
| Implementing all security at the SQL endpoint only | Spark readers bypass it entirely |
| Skipping `OPTIMIZE` / `VACUUM` | Small files accumulate; sync lag, query latency, and storage all degrade |
| `VACUUM` retention shorter than the rollback window | Destroys the ability to recover |
| Foreign key constraints on SQL endpoint tables | Blocks all further schema changes on those tables |
| One workspace holding dozens of lakehouses | Metadata discovery is per-workspace; sync latency climbs |
| Treating the migration as a rewrite-and-improve exercise | Doubles the change surface and makes parity unprovable |
| Deleting the source Warehouse at cutover | Removes the only cheap rollback path |
