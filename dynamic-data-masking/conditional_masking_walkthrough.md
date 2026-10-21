# Conditional Masking — Walkthrough Guide

Narration guide for **Approach 1**: masking that depends on a row-level condition (mask the
SSN only for employees who are also students). Fabric has **no native support** for this, so
both methods below are custom implementations.

**Assets:** `data_masking_spark_files_nb` (Spark) · `tsql_data_mask_ssn_nb` (T-SQL view +
role) · `data-masking-fabric.excalidraw` · `images/Conditional-masking-overview.png`

---

## 1. Product feature vs. custom code — state this first

| Element | Owner | Maintenance burden |
|---|---|---|
| `CREATE VIEW`, `CREATE ROLE`, `GRANT`/`DENY` | Microsoft (T-SQL primitives) | None |
| `EXECUTE AS` / permission evaluation | Microsoft | None |
| Native `ADD MASKED WITH` | Microsoft | None — **but not used in this approach** |
| **The masking rule itself** (`EXISTS` subquery deciding which rows to mask) | **Adopting team** | Full |
| **The mask format function** `sec.fn_mask_ssn` | **Adopting team** | Full |
| **The `DENY` set that prevents bypass** | **Adopting team** | Full — must be re-checked whenever a table, view, or shortcut is added |
| **Spark transformation logic** | **Adopting team** | Full |

Say plainly: *"Everything that makes the masking conditional is code you own. Fabric provides
the plumbing, not the rule."* This is the single most important framing point of the session,
because it determines who is accountable when the rule drifts from the policy.

---

## 2. Method A — mask in Spark (transform the data)

**File:** `data_masking_spark_files_nb`

**Flow**

1. Read `employee` and `student` from the bronze lakehouse.
2. Inner-join on `social_security_number` to identify employees who are also students.
3. Collect those `employee_id` values into a driver-side list.
4. Rewrite the SSN for only those IDs:

```python
employee_df = employee_df.withColumn(
    "social_security_number",
    F.when(
        F.col("employee_id").isin(emp_id_mask_list),
        F.concat(F.lit("XXX-XX-"), F.substring(F.col("social_security_number"), -4, 4))
    ).otherwise(F.col("social_security_number"))
)
```

The notebook also demonstrates two stricter postures: filtering the student rows out
entirely, and dropping the column altogether.

**Properties**

- Masking is **baked into the persisted output**. Every downstream consumer — Spark, SQL
  endpoint, semantic model, shortcut — sees masked values. This is the *only* pattern in this
  folder that covers all access paths.
- To serve both masked and unmasked audiences you must maintain **two copies** of the table,
  with two sets of access controls and a divergence risk between them.
- The raw source remains unmasked. Anyone with access to bronze still sees real values.

**Scale warning — `.isin()` on a collected list**

Steps 3–4 collect matching IDs to the driver and embed them in the query plan. This is fine
for a demo and **breaks down at production volume**:

| Overlap size | Behaviour |
|---|---|
| Thousands | Works; plan gets large |
| Tens of thousands | Driver memory pressure; plan compilation slows noticeably |
| Hundreds of thousands+ | Driver OOM or unusable query plans |

**Production-shaped rewrite** — replace the collect with a left semi/anti join so the work
stays distributed:

```python
mask_targets = (
    employee_df.join(student_df, on="social_security_number", how="left_semi")
               .select("employee_id")
               .withColumn("must_mask", F.lit(True))
)

employee_df = (
    employee_df.join(mask_targets, on="employee_id", how="left")
               .withColumn(
                   "social_security_number",
                   F.when(
                       F.col("must_mask"),
                       F.concat(F.lit("XXX-XX-"),
                                F.substring(F.col("social_security_number"), -4, 4))
                   ).otherwise(F.col("social_security_number"))
               )
               .drop("must_mask")
)
```

---

## 3. Method B — mask at the SQL endpoint (view + role + DENY)

**File:** `tsql_data_mask_ssn_nb` — the recommended conditional pattern, because the base data
stays single-copy and masking is enforced by permissions rather than duplication.

**Flow**

1. Create a `sec` schema to group the security objects. *Organisational only — masking does
   not require a separate schema.*
2. Create `sec.fn_mask_ssn(@ssn)`, returning `XXX-XX-<last 4>`, with NULL and short-input
   guards.
3. Create `sec.vw_employee_masked`, which applies the row condition and calls the function for
   the mask format. **The function is the single definition of the mask format; the view
   supplies only the condition.**
4. Create the `maskedReaders` role: `GRANT SELECT` on the view, `DENY SELECT` on
   `dbo.employee` and `dbo.student`.
5. Add the **Entra security group** to the role.
6. Verify (see §5).

**Why the `DENY` statements are load-bearing**

The mask exists only inside the view. If a `maskedReaders` member could `SELECT` from
`dbo.employee` or `dbo.student` directly, the mask is irrelevant — they read the real value.
The `DENY` grants close that bypass, and `DENY` wins over any conflicting `GRANT`.

**What `DENY` does *not* close:** OneLake, Spark, shortcuts, and Direct Lake. `DENY SELECT` is
a T-SQL permission and has no effect on the Delta file layer. See
`ddm_security_posture.md` §3.

---

## 4. Performance and maintainability of the view pattern

The correlated `EXISTS` runs for every row scanned:

```sql
WHERE EXISTS (SELECT 1 FROM student
              WHERE student.social_security_number = employee.social_security_number)
```

**Performance considerations**

| Concern | Effect | Mitigation |
|---|---|---|
| Semi-join on every query | Cost scales with `employee` × selectivity of `student`; the entire `student` table participates | Materialise the overlap set (see below) |
| String join key | SSN is `VARCHAR`; string comparison is more expensive than an integer key and is collation-sensitive | Join on a surrogate/hashed key |
| No predicate pushdown through the mask | The `CASE` is computed per row after the condition resolves; filtering on the masked column cannot use statistics on the base column | Filter on unmasked keys, not the masked output |
| View is not materialised | The join re-executes on every query, including dashboard refreshes and semantic-model queries | Materialised view or a precomputed flag column |
| Fan-out risk | If a person appears multiple times in `student`, `EXISTS` is safe — but rewriting it as a `JOIN` would duplicate employee rows | Keep `EXISTS`; do not "optimise" it into a join |

**Recommended production shape** — precompute the decision once during the silver load:

```sql
-- Set is_student during the pipeline, not at query time
SELECT e.id, e.first_name, e.last_name,
       CASE WHEN e.is_student = 1
            THEN sec.fn_mask_ssn(e.social_security_number)
            ELSE e.social_security_number
       END AS social_security_number
FROM dbo.employee AS e;
```

This turns a per-query semi-join into a per-row column read, and makes the rule auditable as
data rather than as query logic.

**Maintainability considerations**

| Risk | Why it matters | Mitigation |
|---|---|---|
| **New table or view escapes the DENY set** | The `DENY` list is enumerated by hand. Any new object exposing `social_security_number` is unmasked by default | Deny at **schema** scope; add a CI check that fails when a new object exposes the column |
| **Rule drift** | The masking rule lives in view DDL. A change to the policy requires a code change, review, and deployment | Keep the view in source control (it is, here) and require review on the `sec` schema |
| **Column added to the base table** | `SELECT`-list views do not auto-inherit new columns; a new sensitive column silently never appears — or is added unmasked | Explicit column lists (used here) plus schema-change review |
| **Function/view divergence** | If the view inlines the mask format instead of calling the function, two definitions drift apart | Fixed: the view now calls `sec.fn_mask_ssn` |
| **Role membership sprawl** | Individual users in the role rot as people move | Fixed: role holds an **Entra group**, managed in Entra ID |
| **Silent bypass via shortcut** | A shortcut to the lakehouse from another workspace exposes raw Delta | OneLake data access roles; review shortcut creation |

---

## 5. Demo data and verification

**Demo dataset requirements** — the conditional behaviour is only visible if the data contains
all three cases:

| Case | Purpose | Expected output |
|---|---|---|
| Employee **only** (no student match) | Proves masking is *not* blanket | Full SSN |
| Employee **and** student (SSN matches) | The masking rule fires | `XXX-XX-####` |
| Student **only** (not an employee) | Proves the join direction is correct | Absent from `sec.vw_employee_masked` |

Show at least two rows of each, side by side, so the difference is unmistakable. A single
masked column with no unmasked comparator does not demonstrate *conditional* behaviour.

**Verification script** — run **as a member of the masked group**, never as an admin:

| Step | Query | Expected |
|---|---|---|
| 1 | `SELECT * FROM sec.vw_employee_masked` | Succeeds; mixed masked/unmasked rows |
| 2 | `SELECT * FROM dbo.employee` | **Permission error** |
| 3 | `SELECT * FROM dbo.student` | **Permission error** |
| 4 | Read the Delta table from a Spark notebook | **Should fail** — if it succeeds, OneLake access is not restricted and the mask is bypassable |
| 5 | Open any Direct Lake semantic model over the same lakehouse | **Should not expose raw SSN** — if it does, the semantic model needs its own OLS/RLS |

Steps 4 and 5 are the ones that usually fail. They are the point of the exercise.

---

## 6. Design note: the join key is the sensitive value

`employee` and `student` are joined on `social_security_number` itself. Consequences:

- Any principal able to run that join sees raw SSNs — including anyone debugging the view.
- The overlap set (who is both employee and student) is itself derived from the sensitive
  value, so the derivation is as sensitive as the value.
- Referential quality problems (formatting differences, leading zeros, NULLs) silently change
  who gets masked. A formatting mismatch is a **policy failure**, not a data-quality nuisance.

**Recommendation:** introduce a surrogate person key (or a salted hash of the identifier) in
bronze/silver and join on that. The masking rule then never touches the sensitive value, and
the join becomes testable without exposing raw data.

---

## 7. Narration sequence for the session

1. Show the scenario diagram and state the requirement: mask **only** for the overlapping
   population.
2. State that native DDM cannot do this, and why (four functions, none conditional) —
   see `ms_learn_reconciliation.md` §2.
3. Show Method A (Spark). Note it covers every access path but forces two copies of the data.
4. Show Method B (view + role). Note it keeps one copy but only covers the SQL surface.
5. Run the verification table in §5 **as a non-privileged principal**. Let steps 4 and 5 fail
   in front of the audience — that failure is the lesson.
6. Cover the performance and maintainability tables in §4 before any adoption decision.
7. Close on §6: the join key choice is a design decision that should be made now, not later.
