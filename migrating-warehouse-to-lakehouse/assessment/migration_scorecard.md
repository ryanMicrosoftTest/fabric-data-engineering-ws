# Migration Scorecard

A weighted, fill-in scorecard for deciding whether to migrate a Fabric Warehouse to a
Lakehouse. Referenced from [`../01-decision-framework.md`](../01-decision-framework.md)
§6.

Copy this file per candidate workload. Replace the `_` placeholders. Keep the evidence
column populated — an unsupported score is not reviewable.

---

## Header

| Field | Value |
|---|---|
| Workload / domain | `_` |
| Assessed by | `_` |
| Date | `_` |
| Source Warehouse (item name) | `_` |
| Candidate target pattern (see 08) | `_` A / B / C / D |
| Inventory run date | `_` |

---

## How to score

Each dimension is scored **0–5**, where the score answers *"how well does a Lakehouse
serve this dimension for this workload?"*

| Score | Meaning |
|---|---|
| 5 | Lakehouse is clearly better; no mitigation needed |
| 4 | Lakehouse is better; minor mitigation |
| 3 | Neutral; either works |
| 2 | Lakehouse is worse; mitigation is known and affordable |
| 1 | Lakehouse is worse; mitigation is expensive or fragile |
| 0 | Hard blocker; no acceptable mitigation exists |

**Any dimension scoring 0 ends the assessment.** Record it and choose "stay" or
"hybrid". A weighted total cannot outvote a blocker.

Weights sum to 100. Adjust them if your context genuinely differs, but record the
reason — reweighting after seeing the scores invalidates the exercise.

---

## 1. Transformation logic portability — weight 25

> How much of the existing T-SQL transformation logic can move without redesign?

| Input (from `warehouse_inventory.sql`) | Count |
|---|---|
| Total stored procedures | `_` |
| Procedures classified `BLOCKER` (write operations) | `_` |
| Procedures using `MERGE` | `_` |
| Procedures using `COPY INTO` | `_` |
| Procedures using cursors or `WHILE` loops | `_` |
| Procedures using dynamic SQL | `_` |
| Procedures using temp tables | `_` |
| Tables with `IDENTITY` columns | `_` |
| Computed columns | `_` |

**Scoring guidance**

| Condition | Score |
|---|---|
| Logic is already Spark/Python, or trivially expressible as declarative SQL (MLV candidates) | 5 |
| Mostly set-based SQL; few procedural constructs; no `IDENTITY` | 4 |
| Set-based SQL with some `MERGE` and surrogate keys | 3 |
| Heavy procedural logic, cursors, dynamic SQL, many `IDENTITY` columns | 2 |
| Large body of interdependent procedural logic with no owner who understands it | 1 |
| Logic cannot be rewritten within any acceptable budget | 0 |

| | |
|---|---|
| **Score (0–5)** | `_` |
| **Weighted (score × 5)** | `_` |
| **Evidence** | `_` |

---

## 2. Transactional requirements — weight 20

> Does anything depend on multi-table atomicity or T-SQL write paths?

| Question | Answer |
|---|---|
| Do any loads update two or more tables atomically? | `_` |
| Does any consumer write back through T-SQL? | `_` |
| Are there explicit `BEGIN TRAN` blocks spanning tables? | `_` |
| Is `IDENTITY`-based surrogate key generation relied on downstream? | `_` |
| Is zero-copy clone or warehouse snapshot in use? | `_` |

**Scoring guidance**

| Condition | Score |
|---|---|
| No multi-table transactions; no T-SQL writes; append-only or full-reload patterns | 5 |
| Single-table atomic writes only; Delta transactions are sufficient | 4 |
| Multi-table writes exist but can be restructured around a batch-control pattern | 3 |
| Multi-table atomicity is genuinely required but limited to the gold layer | 2 |
| Multi-table atomicity is pervasive | 1 |
| A T-SQL write path is a non-negotiable requirement | 0 |

> If this scores 0–2, **Pattern B (hybrid)** from `08-reference-architecture.md` is
> almost always the correct answer rather than a full migration.

| | |
|---|---|
| **Score (0–5)** | `_` |
| **Weighted (score × 4)** | `_` |
| **Evidence** | `_` |

---

## 3. Security parity — weight 20

> Can the existing security model be reproduced without widening access?

| Input (from `warehouse_inventory.sql`) | Count |
|---|---|
| Row-level security policies | `_` |
| Masked columns (DDM) | `_` |
| Column-level grants | `_` |
| Distinct principals with explicit grants | `_` |
| Principals that are service principals | `_` |

| Question | Answer |
|---|---|
| Will any user or process have Spark/OneLake access to the target lakehouse? | `_` |
| Chosen access mode (user identity / delegated identity)? | `_` |
| Are shortcuts required into tables that carry OneLake RLS/CLS/OLS? | `_` |
| Are nested or transitive group memberships relied on for access? | `_` |

**Scoring guidance**

| Condition | Score |
|---|---|
| No RLS, no CLS, no DDM; item-level permissions only | 5 |
| Simple CLS or object-level grants only | 4 |
| RLS present, reproducible in the chosen access mode | 3 |
| DDM present — requires delegated identity mode or write-time masking | 2 |
| Mixed requirements that force contradictory access modes on one item | 1 |
| Reproducing the model would require granting broader data access than today | 0 |

> **Remember:** SQL-endpoint security applies only through the endpoint. Any principal
> with Spark or OneLake access to the lakehouse bypasses it. If that is unacceptable
> and cannot be prevented, this is a 0.

| | |
|---|---|
| **Score (0–5)** | `_` |
| **Weighted (score × 4)** | `_` |
| **Evidence** | `_` |

---

## 4. Consumption-layer impact — weight 15

> What breaks for report authors, analysts, and downstream applications?

| Question | Answer |
|---|---|
| Number of semantic models over the source | `_` |
| Number using Direct Lake | `_` |
| Number of semantic model tables built on non-materialized SQL views | `_` |
| Number of reports | `_` |
| Number of external applications with hard-coded connection strings | `_` |
| Are there Excel / ad-hoc users connecting directly? | `_` |
| Is sub-minute data freshness expected by any consumer? | `_` |

**Scoring guidance**

| Condition | Score |
|---|---|
| Few consumers; all centrally managed; connection details parameterized | 5 |
| Moderate consumer count; all managed by the platform team | 4 |
| Many consumers but inventoried and reachable | 3 |
| Unknown ad-hoc consumers; hard-coded connection strings present | 2 |
| Direct Lake models that would fall back to DirectQuery, with no headroom | 1 |
| A consumer requirement cannot be met at all (e.g. SQL write-back from a report) | 0 |

> Note the metadata-sync consideration: without the new sync, Lakehouse SQL endpoint
> freshness is eventually consistent after Spark writes. Pipelines should explicitly
> refresh the endpoint before downstream reads.

| | |
|---|---|
| **Score (0–5)** | `_` |
| **Weighted (score × 3)** | `_` |
| **Evidence** | `_` |

---

## 5. Team skills and operating model — weight 10

> Can the team build and, more importantly, operate the target?

| Question | Answer |
|---|---|
| Team members fluent in Spark / PySpark | `_` of `_` |
| Team members fluent in T-SQL only | `_` of `_` |
| Existing CI/CD for notebooks? | `_` |
| Who will own `OPTIMIZE` / `VACUUM` scheduling after go-live? | `_` |
| Is there an on-call rotation that can debug a Spark job at 3 a.m.? | `_` |

**Scoring guidance**

| Condition | Score |
|---|---|
| Established Spark practice with CI/CD, testing, and operational ownership | 5 |
| Spark skills present; operations formalizable during the project | 4 |
| Mixed skills; training planned and funded | 3 |
| T-SQL-only team with a willingness to learn but no funded plan | 2 |
| T-SQL-only team; no capacity for new operational burden | 1 |
| No one will own layout maintenance after go-live | 0 |

> The Warehouse manages layout for you. The Lakehouse does not. If no one owns
> maintenance, performance and cost will degrade silently.

| | |
|---|---|
| **Score (0–5)** | `_` |
| **Weighted (score × 2)** | `_` |
| **Evidence** | `_` |

---

## 6. Interoperability requirement strength — weight 10

> How real and how strong is the external-engine or external-data requirement?

| Question | Answer |
|---|---|
| Named external engines that must read the data | `_` |
| Have you tested whether they can read the **Warehouse's** OneLake Delta output? | `_` |
| External data that must be accessed without copying | `_` |
| Is the requirement contractual, architectural, or aspirational? | `_` |

**Scoring guidance**

| Condition | Score |
|---|---|
| Hard, named, tested requirement that only a Lakehouse satisfies | 5 |
| Strong requirement with a tested Lakehouse-only path | 4 |
| Real requirement, partially satisfiable by shortcuts into the existing estate | 3 |
| Requirement satisfiable by Pattern D (federation lakehouse) without migrating | 2 |
| Requirement is aspirational and untested | 1 |
| n/a — scoring 0 here is not a blocker; use 1 | — |

> If this dimension is the *primary* driver and scores 2, build Pattern D and stop.

| | |
|---|---|
| **Score (0–5)** | `_` |
| **Weighted (score × 2)** | `_` |
| **Evidence** | `_` |

---

## Total

| Dimension | Weight | Score (0–5) | Weighted |
|---|---:|---:|---:|
| 1. Transformation logic portability | 25 | `_` | `_` |
| 2. Transactional requirements | 20 | `_` | `_` |
| 3. Security parity | 20 | `_` | `_` |
| 4. Consumption-layer impact | 15 | `_` | `_` |
| 5. Team skills and operating model | 10 | `_` | `_` |
| 6. Interoperability requirement strength | 10 | `_` | `_` |
| **Total** | **100** | | **`_` / 100** |

---

## Interpreting the total

| Total | Reading |
|---|---|
| **80–100** | Migrate. Pattern A or C. Risks are manageable. |
| **60–79** | Migrate with care, or adopt Pattern C to minimize rewrite. Expect a long Phase 3. |
| **40–59** | Hybrid (Pattern B) is usually the right answer. A full migration will be expensive relative to its benefit. |
| **20–39** | Stay on Warehouse. Consider Pattern D if the driver is external data access. |
| **0–19** | Stay. Re-examine whether the stated driver is real. |
| **Any dimension = 0** | Blocked, regardless of total. Record the blocker; choose stay or hybrid. |

---

## Decision record

| Field | Value |
|---|---|
| Blockers found (dimension, description) | `_` |
| Total score | `_` |
| **Decision** | `_` migrate / hybrid / stay |
| Target pattern | `_` |
| Driver, in one sentence | `_` |
| Top three accepted risks | 1. `_` 2. `_` 3. `_` |
| Capabilities being given up, and replacements | `_` |
| Measurable success criteria | `_` |
| Decision owner | `_` |
| Reviewers | `_` |
| Review date | `_` |

A score is not a decision. It makes the trade-offs explicit and reviewable — the
decision record above is the actual output.
