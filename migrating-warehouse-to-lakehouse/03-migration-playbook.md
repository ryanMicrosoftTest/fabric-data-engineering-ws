# 03 — Migration Playbook

A phased, reversible execution plan. Each phase has an entry condition, activities, and
an exit condition. Do not start a phase until the previous exit condition is met.

The playbook uses a **blue/green** approach: the source Warehouse ("blue") stays live
and authoritative until cutover. The target Lakehouse ("green") is built alongside it.

---

## Phase overview

| Phase | Name | Typical duration | Reversible? |
|---|---|---|---|
| 0 | Assessment and inventory | 1–2 weeks | n/a |
| 1 | Target design | 1–2 weeks | n/a |
| 2 | Provision and secure the target | 2–4 days | Yes |
| 3 | Transformation logic rewrite | **Dominant effort** | Yes |
| 4 | Data backfill and dual-run | 1–2 weeks | Yes |
| 5 | Consumption layer migration | 1–2 weeks | Yes |
| 6 | Parity validation | 1–2 weeks | Yes |
| 7 | Cutover | 1 day | Config-only rollback |
| 8 | Decommission and hardening | 2–4 weeks after cutover | No |

Phase 3 is almost always 60–80% of total effort. Estimate it from the inventory, not
from intuition.

---

## Phase 0 — Assessment and inventory

**Entry:** a signed decision record from [01-decision-framework.md](01-decision-framework.md).

### Activities

1. Run [`assessment/warehouse_inventory.sql`](assessment/warehouse_inventory.sql)
   against the source Warehouse. Capture:
   - All schemas, tables, row counts, and sizes.
   - All views, stored procedures, functions, and their definitions.
   - All `IDENTITY` columns, constraints, and defaults.
   - All security policies, masked columns, and granular grants.
2. Inventory everything *outside* the Warehouse that touches it:
   - Pipelines and dataflows writing to it.
   - Notebooks and Spark jobs reading from it.
   - Semantic models and reports built on it.
   - External applications, ODBC/JDBC connections, and scheduled extracts.
   - Shortcuts pointing *at* it from other items.
3. Classify every object using the severity key in
   [02-capability-gap-analysis.md](02-capability-gap-analysis.md).
4. Profile data volumes and change rates per table (full reload vs. incremental,
   rows/day, update/delete ratio). This drives Phase 4 design.

### Discovery queries for external dependencies

```text
# In the repo or artifact export, search for references to the source warehouse:
#   - three-part names:        warehouse_name.schema.table
#   - connection strings:      <workspace>.datawarehouse.fabric.microsoft.com
#   - item GUIDs:              the warehouse item ID
#   - T-SQL writes:            INSERT INTO, MERGE, COPY INTO, EXEC usp_
```

### Exit

- A complete object inventory with severity classification.
- A dependency map of every producer and consumer.
- An effort estimate for Phase 3 derived from the count of 🔴 and 🟠 objects.

---

## Phase 1 — Target design

**Entry:** completed inventory.

### Activities

1. **Choose the Lakehouse topology.**
   - One Lakehouse per medallion layer, or one Lakehouse with schemas per layer?
   - Prefer **schema-enabled lakehouses** for anything non-trivial — they give you
     `lakehouse.schema.table` three-part naming parity with the Warehouse.
   - Avoid putting a very large number of lakehouses in a single workspace. Automatic
     metadata discovery is a **single instance per workspace**, so many lakehouses in
     one workspace increases SQL endpoint sync latency. Split across workspaces if
     latency becomes a problem.

2. **Decide the security mode** per Lakehouse — user identity mode or delegated
   identity mode. This is a foundational choice; see
   [05-security-mapping.md](05-security-mapping.md). Changing it later is disruptive.

3. **Decide the write engine** for each transformation:
   - PySpark notebook (most flexible)
   - Spark SQL notebook (closest to existing T-SQL)
   - **Materialized lake views** (declarative; best fit for straightforward
     medallion SQL transformations)
   - Dataflow Gen2 (low-code; good for simple, source-aligned loads)
   - Pipeline Copy activity (pure movement, no transformation)

4. **Design the orchestration.** Warehouse batches that relied on a multi-table
   transaction need an explicit orchestration contract:
   - A `batch_id` / `load_ts` column on every target table.
   - A control table recording batch start, end, and status.
   - Consumers read only completed batches.

5. **Design the naming and layout standard.**
   - Schema per medallion layer (`bronze`, `silver`, `gold`) or per domain.
   - Lower-case, underscore-separated object names.
   - Explicit three-part naming everywhere; never rely on a default lakehouse.

6. **Design the maintenance regime.** Who runs `OPTIMIZE` and `VACUUM`, how often, and
   with what retention. See [06-operations-and-performance.md](06-operations-and-performance.md).

### Exit

- A target architecture diagram.
- A table-by-table mapping: source Warehouse object → target Lakehouse object + schema.
- A written security design.
- A written maintenance and orchestration design.

---

## Phase 2 — Provision and secure the target

**Entry:** approved target design.

### Activities

1. Create the target Lakehouse(s) with schemas enabled, via REST API or deployment
   pipeline — **not** by hand, so the provisioning is reproducible across environments.
2. Create the schemas.
3. Apply the security design:
   - Workspace roles.
   - OneLake security roles (if user identity mode), or SQL grants and policies
     (if delegated identity mode).
   - Service principal access for automation.
4. Configure shortcuts for any external data the Lakehouse should read without copying.
5. Wire up source control and deployment pipelines for the new items.

### Exit

- Target Lakehouse exists in dev, test, and prod with identical structure.
- A non-privileged test account can connect and sees exactly what the design says it
  should see.

---

## Phase 3 — Transformation logic rewrite

**Entry:** provisioned target.

This is the bulk of the work. See
[04-tsql-to-spark-patterns.md](04-tsql-to-spark-patterns.md) for the pattern library.

### Approach

1. **Order the work by dependency**, bronze → silver → gold. Do not rewrite a gold
   aggregate before its silver source exists.
2. **Rewrite in the smallest viable unit.** One stored procedure → one notebook or one
   materialized lake view. Resist the urge to "improve while migrating": the first
   target is *behavioural parity*, not optimisation.
3. **Parameterize everything.** No hard-coded workspace IDs, lakehouse IDs, or
   environment names. Use a variable library or notebook parameters.
4. **Make every job idempotent.** Re-running the same batch must produce the same
   result. This replaces the safety that multi-table transactions used to provide.
5. **Write a parity test alongside each rewrite**, not after. See
   [`assessment/parity_validation.py`](assessment/parity_validation.py).

### Suggested conversion order within each unit

| Step | Action |
|---|---|
| 1 | Extract the T-SQL body and list its inputs and outputs. |
| 2 | Translate set-based logic to Spark SQL as literally as possible. |
| 3 | Replace `MERGE` with Delta `MERGE INTO`. |
| 4 | Replace `IDENTITY` with a deterministic surrogate key strategy. |
| 5 | Replace temp tables with temp views or cached DataFrames. |
| 6 | Replace procedural loops/cursors with set-based or windowed logic. |
| 7 | Add `batch_id` / `load_ts` columns. |
| 8 | Add the parity test. |

### Exit

- Every 🔴 and 🟠 object has a working replacement in dev.
- Every replacement has a passing parity test against a dev copy of the data.

---

## Phase 4 — Data backfill and dual-run

**Entry:** rewritten logic passing parity tests in dev.

### Activities

1. **Backfill** historical data from the Warehouse into the Lakehouse. Options, in
   order of preference:
   - Read the Warehouse via Spark (three-part name) and write Delta to the Lakehouse.
   - Pipeline Copy activity, Warehouse → Lakehouse table.
   - Shortcut the Warehouse's OneLake Delta files and `CONVERT`/copy.
   Partition large backfills by date and run them in waves.
2. **Dual-run.** Keep the Warehouse pipelines running as the system of record while the
   new Lakehouse pipelines run in parallel on the same source data.
   - Both write; only the Warehouse is consumed.
   - Run for at least one full business cycle (a month-end if your data has one).
3. **Compare continuously.** Schedule the parity validation job after every dual-run
   cycle and alert on any variance.

### Single-write-truth rule

At any moment, exactly one system must be authoritative for a given table. During
dual-run, that is the Warehouse. Never let consumers read from both, or you will chase
phantom discrepancies.

### Exit

- Backfill complete, with row counts and checksums matching.
- At least one full business cycle of dual-run with zero unexplained variance.

---

## Phase 5 — Consumption layer migration

**Entry:** clean dual-run.

### Activities

1. **Semantic models.**
   - Create new Direct Lake semantic models over the Lakehouse (prefer **Direct Lake
     on OneLake** for new models).
   - Re-point or rebuild measures, relationships, RLS roles, and perspectives.
   - Verify no table breaches Direct Lake guardrails.
   - Verify no table is sourced from a non-materialized SQL view — convert those to
     materialized lake views.
2. **Reports and dashboards.** Rebind to the new semantic model. Validate visuals,
   drill-through, and bookmarks.
3. **Subscriptions, alerts, and apps.** Re-create and re-point.
4. **External consumers.** Issue new connection strings for the Lakehouse SQL analytics
   endpoint. Flag any consumer that writes — those need a different solution entirely.
5. **Shortcuts pointing at the old Warehouse.** Re-point to the Lakehouse.

### Exit

- Every consumer has a validated equivalent on the Lakehouse.
- A side-by-side report comparison signed off by a data owner.

---

## Phase 6 — Parity validation

**Entry:** migrated consumption layer.

See [07-validation-and-cutover.md](07-validation-and-cutover.md) for the full checklist.
In short, validate four layers:

1. **Data** — row counts, column-level checksums, null distributions, min/max ranges.
2. **Logic** — reconcile key business aggregates to the decimal.
3. **Security** — test with representative non-privileged accounts, through *every*
   access path (SQL endpoint, Spark, Power BI).
4. **Performance** — p50/p95 query latency and refresh duration vs. baseline.

### Exit

A signed validation report with no open 🔴 defects.

---

## Phase 7 — Cutover

**Entry:** signed validation report.

See [07-validation-and-cutover.md](07-validation-and-cutover.md) for the detailed
sequence. Headlines:

- Pick a low-activity window after a successful batch.
- Freeze writes to the Warehouse.
- Run a final incremental sync.
- Flip consumer connections.
- Set the Warehouse to read-only (revoke write grants) rather than deleting it.
- Monitor intensively for one full business cycle.

### Exit

All consumers on the Lakehouse; Warehouse read-only but intact.

---

## Phase 8 — Decommission and hardening

**Entry:** one clean business cycle post-cutover.

### Activities

1. Keep the Warehouse read-only for an agreed grace period (30–90 days is typical).
2. Stand up the ongoing maintenance regime: scheduled `OPTIMIZE`, `VACUUM`, and SQL
   endpoint metadata refresh.
3. Establish monitoring: pipeline success, refresh duration, CU consumption, Direct Lake
   fallback rate, small-file counts.
4. Document the new operating model and on-call runbook.
5. Only then, archive and delete the Warehouse.

---

## Rollback posture by phase

| Phase | Rollback |
|---|---|
| 0–3 | Nothing in production changed. Abandon freely. |
| 4 | Stop Lakehouse pipelines. Warehouse never stopped being authoritative. |
| 5 | Consumers still pointed at Warehouse-backed models; revert bindings. |
| 6 | Same as 5. |
| 7 | **Config-only rollback**: re-point consumers, re-enable Warehouse writes. Clean if done before any Lakehouse-only write has been consumed. |
| 8 | **Post-write rollback**: requires replaying Lakehouse-era changes into the Warehouse. Expensive. This is why the grace period exists. |
