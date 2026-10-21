# 07 — Validation and Cutover

Parity is proven, not assumed. This document defines what "done" means and how to flip
production safely.

---

## 1. Validation layers

Validate four layers, in order. Do not proceed up the stack while a lower layer fails.

```
4. Performance   — is it fast enough?
3. Security      — does the right person see the right rows, on every path?
2. Logic         — do the business numbers reconcile?
1. Data          — is the data identical?
```

---

## 2. Layer 1 — Data validation

Run [`assessment/parity_validation.py`](assessment/parity_validation.py). It checks:

| Check | Pass criterion |
|---|---|
| Table presence | Every source table exists in the target |
| Row count | Exact match per table |
| Column presence | Every source column exists in the target |
| Data type compatibility | Each column maps to a compatible T-SQL type |
| Null count per column | Exact match |
| Distinct count per key column | Exact match |
| Min / max per numeric and date column | Exact match |
| Sum per numeric column | Match within a declared tolerance (default: exact) |
| Row-level hash over all columns | Set of hashes identical |

### Numeric tolerance

Default to **exact** equality. Only relax it where you can name the reason — for example
a documented decimal-precision difference. Every tolerance must be recorded with a
justification; an unexplained tolerance is a hidden defect.

### Common sources of genuine drift

| Symptom | Likely cause |
|---|---|
| Timestamps off by a fixed offset | `spark.sql.session.timeZone` not set to UTC |
| Decimal sums differ in the last places | Implicit cast / precision differences between engines |
| Trailing-whitespace mismatches | `TRIM` vs. `LTRIM(RTRIM())` semantics on non-space whitespace |
| String comparison differences | Collation: T-SQL is often case-insensitive, Spark is case-sensitive |
| Row count higher in target | Non-idempotent load re-run |
| A column missing in the SQL endpoint | Unsupported Delta type, or a foreign key constraint blocking schema refresh |
| A table missing entirely | Not Delta, not under `Tables/`, or an external Spark table needing a shortcut |

> ⚠️ **Collation is the quiet one.** A `GROUP BY customer_name` that merged `ACME` and
> `Acme` in T-SQL will produce two groups in Spark. Check every string-keyed aggregate.

---

## 3. Layer 2 — Logic validation

Data parity is necessary but not sufficient. Reconcile the **business** outputs.

| Artefact | Validation |
|---|---|
| Key KPIs | Reconcile to the decimal against the Warehouse for the same period |
| Period-over-period aggregates | Run for at least 13 months to catch calendar/fiscal logic |
| Edge-case records | Nulls, negative amounts, late-arriving facts, orphaned dimension keys |
| Deleted/restated records | Confirm SCD and soft-delete behaviour matches |
| Reconciliation across grains | Gold totals must tie to silver totals must tie to bronze |

Have a **data owner**, not just an engineer, sign off this layer.

---

## 4. Layer 3 — Security validation

Execute the full matrix from [05-security-mapping.md](05-security-mapping.md) §8.

Every test must be run through **all four access paths**:

1. SQL analytics endpoint (SSMS or the Fabric SQL editor).
2. Spark notebook.
3. Direct Lake semantic model / Power BI report.
4. OneLake file API.

A rule that holds on one path and not another is a **finding**, not a nuance. Record it,
assign a compensating control, and get explicit acceptance.

### Minimum test set

- [ ] A restricted analyst sees only their permitted rows, on all four paths.
- [ ] A restricted analyst cannot see restricted columns, on all four paths.
- [ ] Masked values are masked, on all four paths (or the gap is formally accepted).
- [ ] A user with no item Read permission cannot connect to the endpoint at all.
- [ ] A service principal used by pipelines can write but not read restricted data it
      should not.
- [ ] Removing a user from a group removes their access within the expected interval.
- [ ] Shortcut-backed tables behave as designed under the chosen access mode.

---

## 5. Layer 4 — Performance validation

| Metric | Baseline source | Pass criterion |
|---|---|---|
| p50 / p95 interactive query latency | Warehouse query history | Within agreed delta of baseline |
| End-to-end ETL duration | Warehouse pipeline history | Within agreed SLA |
| Semantic model framing duration | New measurement | Seconds |
| Report render time, top 10 reports | Power BI performance analyzer | Within agreed delta |
| DirectQuery fallback occurrences | Semantic model telemetry | **Zero** for Direct Lake models |
| Peak CU during ETL window | Capacity Metrics | Within capacity headroom |
| SQL endpoint freshness lag after a write | Write-then-read probe | Within agreed freshness SLA |

Measure under **representative concurrency**, not a single user. A single-user test
proves almost nothing about a BI workload.

---

## 6. Pre-cutover checklist

Everything below must be true before you schedule the window.

### Data
- [ ] Backfill complete; parity validation passing with zero unexplained variance.
- [ ] At least one full business cycle of clean dual-run.
- [ ] Incremental sync tested and timed.

### Logic
- [ ] Every 🔴 and 🟠 object from the gap analysis has a working, tested replacement.
- [ ] Every rewritten job is idempotent and has been re-run to prove it.
- [ ] Batch control table and completion markers in place.

### Consumption
- [ ] All semantic models rebuilt and validated.
- [ ] All reports rebound and visually compared side by side.
- [ ] Subscriptions, alerts, and apps re-created.
- [ ] Every external consumer has a new connection string and has tested it.
- [ ] Every write-back consumer has been identified and re-platformed or retired.
- [ ] Shortcuts pointing at the old Warehouse re-pointed.

### Operations
- [ ] Maintenance jobs (`OPTIMIZE`, `VACUUM`) scheduled and tested.
- [ ] SQL endpoint metadata refresh wired into every writing pipeline.
- [ ] Monitoring and alerting live.
- [ ] Runbook written and walked through with the on-call team.

### Governance
- [ ] Security matrix fully green, or gaps formally accepted in writing.
- [ ] Rollback plan written, with named decision-maker and trigger thresholds.
- [ ] Grace period agreed (30–90 days typical).
- [ ] Communication sent to all consumers with date, time, and expected impact.

---

## 7. Cutover sequence

Pick a low-activity window **immediately after a successful batch**, and avoid
period-end.

| # | Step | Owner | Rollback point |
|---|---|---|---|
| 1 | Announce start; freeze change control | Lead | — |
| 2 | Disable Warehouse-writing pipelines | Eng | Re-enable |
| 3 | Confirm no in-flight Warehouse writes | Eng | — |
| 4 | Run final incremental sync into the Lakehouse | Eng | — |
| 5 | Run parity validation; **hard gate** | Eng | **Abort here if it fails** |
| 6 | Run `OPTIMIZE` on gold tables | Eng | — |
| 7 | Force SQL endpoint metadata refresh | Eng | — |
| 8 | Frame all Direct Lake semantic models | BI | — |
| 9 | Smoke-test top reports and key queries | BI | Re-point to old models |
| 10 | Re-point external consumers | Eng | Re-point back |
| 11 | Enable Lakehouse pipelines on the production schedule | Eng | Disable |
| 12 | **Revoke write permissions on the Warehouse** (do not delete) | Admin | Re-grant |
| 13 | Run the security test matrix spot-check | Sec | — |
| 14 | Announce complete; begin heightened monitoring | Lead | — |

**Do not delete the Warehouse.** Making it read-only preserves a complete, instant
rollback target for the entire grace period at the cost of storage alone.

---

## 8. Post-cutover monitoring

| Window | Focus |
|---|---|
| First 4 hours | Pipeline success, report availability, obvious errors |
| First 24 hours | Full overnight batch; refresh durations; CU spike |
| First week | Query latency trend, small-file growth, fallback events, user-reported discrepancies |
| First month | A complete business cycle including period-end; storage growth; maintenance job effectiveness |

Run the parity validation job daily for the first week against the (now read-only)
Warehouse, comparing only tables that should not have changed.

---

## 9. Rollback

### 9.1 Classes

| Class | Condition | Cost |
|---|---|---|
| **Config-only** | No Lakehouse-only write has been consumed by a downstream system of record | Low — re-point and re-enable |
| **Post-write** | Lakehouse-era data has been consumed or exported downstream | High — requires replay into the Warehouse |

### 9.2 Config-only rollback

1. Disable Lakehouse pipelines.
2. Re-grant write permissions on the Warehouse.
3. Re-enable Warehouse pipelines.
4. Re-point semantic models and external consumers.
5. Run a catch-up load into the Warehouse for the gap period.
6. Validate, then communicate.

Target: under two hours if rehearsed. **Rehearse it in test.**

### 9.3 Decision framework

| Trigger | Action |
|---|---|
| Parity validation fails at cutover step 5 | **Abort.** Do not proceed. |
| Critical report unavailable > 2 hours | Roll back |
| Data correctness defect affecting a reported KPI | Roll back |
| Performance regression > agreed threshold, no fix within 24h | Roll back |
| Security finding exposing restricted data | **Roll back immediately** |
| Cosmetic or single-report issue | Fix forward |
| Non-critical pipeline failure with a manual workaround | Fix forward |

Name the decision-maker **before** cutover. Rollback decisions made by committee at
02:00 go badly.

### 9.4 Grace period

Keep the Warehouse read-only for the agreed period (30–90 days). During it:

- Do not delete anything.
- Keep `VACUUM` retention on the Lakehouse at or above the grace period, so Delta time
  travel can reach any point in it.
- Keep the rollback runbook current.

Only after a clean grace period should you archive and delete the Warehouse.

---

## 10. Sign-off record

| Layer | Evidence | Approver role | Date | Signature |
|---|---|---|---|---|
| Data parity | Parity validation report | Data engineering lead | | |
| Logic parity | KPI reconciliation pack | Data owner / business lead | | |
| Security | Completed security test matrix | Security / governance lead | | |
| Performance | Benchmark comparison | Platform lead | | |
| Operational readiness | Runbook + monitoring evidence | Operations lead | | |
| Rollback readiness | Rehearsal record | Migration lead | | |

No cutover without all six.
