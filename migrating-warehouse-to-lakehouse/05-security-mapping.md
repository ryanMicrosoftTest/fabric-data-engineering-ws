# 05 — Security Mapping

Security is the dimension where Warehouse-to-Lakehouse migrations most often go wrong,
because the Lakehouse introduces a second data path that the Warehouse never had.

---

## 1. The fundamental difference

In a **Warehouse**, T-SQL is the only way to read the data, so SQL security *is* the
security model.

In a **Lakehouse**, there are multiple read paths:

```
                    ┌────────────────────────────┐
                    │        Delta tables        │
                    │        in OneLake          │
                    └────────────┬───────────────┘
                                 │
        ┌────────────┬───────────┼────────────┬──────────────┐
        │            │           │            │              │
   SQL analytics   Spark     OneLake APIs  Direct Lake   External engines
     endpoint    notebooks   / ADLS API    semantic       via shortcut
                                            models
```

> 🔴 **SQL security rules set on the SQL analytics endpoint apply only when data is
> accessed through that endpoint.** They do not apply when the same data is read through
> Spark or other tools.

If you lift a Warehouse's RLS/CLS/DDM design into the Lakehouse SQL endpoint and stop
there, anyone with workspace access and a notebook can bypass it entirely.

---

## 2. Choose an access mode first

The Lakehouse SQL analytics endpoint supports two mutually exclusive access modes.
This is a foundational decision — make it in Phase 1 of the playbook.

### 2.1 User identity mode

The signed-in user's Entra identity is passed through to OneLake, and **OneLake security
rules govern all table reads**.

- Table access is governed entirely by OneLake security roles.
- SQL `GRANT`/`REVOKE` statements **on tables are ignored**.
- RLS, CLS, and object-level security are defined in the OneLake security experience.
- SQL permissions still apply to **non-data objects** — views, stored procedures,
  functions.
- Write operations are not supported at the endpoint; all writes go through the
  lakehouse and are governed by workspace roles (Admin, Member, Contributor).

**Choose this when** you want one security definition enforced consistently across
Power BI, notebooks, the lakehouse, and the SQL endpoint.

### 2.2 Delegated identity mode

The endpoint connects to OneLake using the **item owner's** identity, and **SQL
permissions govern everything**.

- Full control via SQL `GRANT`/`REVOKE` at all object levels.
- RLS via `CREATE SECURITY POLICY`.
- CLS via `GRANT SELECT` with a column list.
- **DDM via `ALTER TABLE ... MASKED WITH ...`.**
- OneLake security roles and access policies **do not carry over** to table-level
  access. Rules enforced by Spark or other OneLake readers will **not** apply at the
  endpoint.

**Choose this when** the workload depends on SQL-native security semantics, or existing
T-SQL tooling and DBA practice require full compatibility. This is the closest match to
a Warehouse's behaviour.

#### Delegated mode caveats

| Caveat | Detail |
|---|---|
| Item-owner identity is used for OneLake reads | The owner must have sufficient OneLake permission to read the underlying files on behalf of the workload. Misalignment between user SQL grants and the owner's OneLake access causes **query failures**. |
| Owner cannot be a service principal | Reassign ownership to a user or group account. |
| Shortcuts are restricted | If the **source** table of a shortcut has *any* OneLake-level RLS, CLS, or OLS rule, the endpoint **blocks access to that shortcut** by design. Shortcuts to sources with no data-level rules work normally. |

---

## 3. Feature-by-feature mapping

| Security target | Warehouse | Lakehouse — user identity mode | Lakehouse — delegated identity mode |
|---|---|---|---|
| **Tables** | SQL `GRANT`/`REVOKE` | OneLake security roles control access. SQL `GRANT`/`REVOKE` **not allowed**. | Full control using SQL `GRANT`/`REVOKE`. |
| **Views** | SQL `GRANT`/`REVOKE` | SQL `GRANT`/`REVOKE` | SQL `GRANT`/`REVOKE` |
| **Stored procedures** | `GRANT EXECUTE` | `GRANT EXECUTE` | `GRANT EXECUTE` |
| **Functions** | `GRANT EXECUTE` | `GRANT EXECUTE` | `GRANT EXECUTE` |
| **Row-level security** | `CREATE SECURITY POLICY` | Defined as part of OneLake security roles | `CREATE SECURITY POLICY` |
| **Column-level security** | `GRANT SELECT` with column list | Defined as part of OneLake security roles | `GRANT SELECT` with column list |
| **Dynamic Data Masking** | `ALTER TABLE` with `MASKED` | **Not supported in OneLake security** | `ALTER TABLE` with `MASKED` |

### 3.1 The DDM gap

Dynamic Data Masking is **not supported in OneLake security.** If your Warehouse relies
on DDM, you have three options:

1. **Use delegated identity mode** and recreate the DDM definitions in SQL. Accept that
   the masking applies only through the SQL endpoint, not through Spark.
2. **Mask at write time.** Produce a masked column in the Delta table itself during
   transformation, and expose only the masked version to general consumers. Keep the
   unmasked column in a separately secured table or schema. This is the only option that
   holds across *all* read paths.
3. **Expose a masking view** and grant access only to the view, not the base table.
   Again, endpoint-only.

> For anything with a genuine regulatory obligation, option 2 — masking at write time,
> with physical separation of the unmasked data — is the only design that survives a
> Spark notebook.

---

## 4. Prerequisite: item-level Read permission

Regardless of mode:

> To connect to and query data through a SQL analytics endpoint, users must have **Read
> permission on the item** associated with the endpoint. If a user has no control-plane
> access to the item (workspace role or explicit item permission), the connection is
> **rejected regardless of any SQL permissions** that might exist for that user.

This trips up migrations that grant SQL permissions correctly but forget item sharing.

---

## 5. OneLake security roles — practical notes

### Row-level security

- Defined as part of any OneLake security role that grants access to Delta Parquet table
  data.
- Applies only to tabular data — you cannot define RLS on non-table folders or
  unstructured data.
- Keep expressions simple. Vague or overly complex RLS expressions are an explicit
  anti-pattern.

### Column-level security

- By default users have access to all columns. CLS rules **hide** columns to revoke
  access.
- ⚠️ **Removing access to a column does not deny access if another role grants it.**
  Permissions are additive across roles. Audit the union, not individual roles.
- At least one column must remain allowed.

### Hub-and-spoke identity mapping

When OneLake security policies are carried from a **producer** item (where the role is
defined) to a **consumer** item (accessing via shortcut), the identities must map
**exactly 1:1**:

- If the producer role references a specific user, that exact user must have Fabric Read
  on the consumer item.
- If the producer role references `Group A`, then **`Group A` itself** must be granted
  Fabric Read on the consumer — granting it to a *member* of Group A does not satisfy
  the match.
- **Nested or effective group membership is not resolved across this boundary.**

---

## 6. Direct Lake security interactions

| Situation | Direct Lake on SQL | Direct Lake on OneLake |
|---|---|---|
| SQL RLS on the endpoint | Queries succeed but **fall back to DirectQuery**; fail if fallback is disabled | Queries succeed, and **SQL RLS is not applied** |
| SQL DDM on the endpoint | Falls back to DirectQuery | Not applicable |
| SQL OLS on the endpoint | Falls back to DirectQuery | Not applicable |
| OneLake security on the source | — | Enforced (requires user access to OneLake files) |

> 🔴 **Read that first row twice.** A Direct Lake on OneLake model over a table with SQL
> RLS defined at the endpoint will return **unfiltered data**, because Direct Lake on
> OneLake requires access to the OneLake files and does not observe SQL-based RLS.

Design rules that follow:

- If your row filtering must hold for Power BI, either implement it in **OneLake
  security** (user identity mode) **or** implement **semantic-model RLS** in the Power BI
  model itself — do not rely on SQL endpoint RLS reaching a Direct Lake on OneLake model.
- Workspace **Viewers** need OneLake security roles granting read on the source items. If
  a source item has shortcuts to another item, users also need read on each shortcut's
  target item.
- To isolate users from the source item entirely, bind the Direct Lake model to a cloud
  connection using a **fixed identity**, with SSO disabled.

---

## 7. Migration procedure for security

1. **Export the current state** from the Warehouse:
   - All `GRANT`/`DENY`/`REVOKE` statements.
   - All security policies and their predicate functions.
   - All masked columns and masking functions.
   - All role memberships.
   (The inventory script in `assessment/` produces this.)

2. **Classify each rule by required enforcement scope:**

   | Scope | Meaning | Target mechanism |
   |---|---|---|
   | SQL-only | Rule only needs to hold for T-SQL consumers | SQL endpoint (delegated mode) |
   | All engines | Rule must hold for Spark, Power BI, and SQL alike | OneLake security (user identity mode) or physical separation |
   | Regulatory | Rule must hold even against a determined internal actor | Physical separation: masked/filtered tables in a separately secured item |

3. **Choose the access mode** based on the dominant class.

4. **Implement**, then **test through every path**:
   - Query the SQL analytics endpoint as a non-privileged test user.
   - Read the same table from a Spark notebook as the same user.
   - Open a Direct Lake report as the same user.
   - Read via OneLake file APIs as the same user.
   All four must produce the expected result. A pass on one is not a pass.

5. **Document the residual risk.** If any rule is endpoint-only, state plainly which
   path bypasses it and who holds the compensating control.

---

## 8. Security test matrix template

| Test ID | Principal | Table | Expected via SQL endpoint | Expected via Spark | Expected via Direct Lake | Expected via OneLake API | Result |
|---|---|---|---|---|---|---|---|
| SEC-01 | `analyst_region_a` | `gold.fact_order` | Region A rows only | Region A rows only | Region A rows only | Region A rows only | |
| SEC-02 | `analyst_region_a` | `gold.dim_customer` | SSN column hidden | SSN column hidden | SSN column hidden | SSN column hidden | |
| SEC-03 | `report_viewer` | `gold.fact_order` | Access denied | Access denied | Aggregates visible | Access denied | |
| SEC-04 | `etl_service_principal` | all `silver.*` | Read/write | Read/write | n/a | Read/write | |
| SEC-05 | `external_partner` | `gold.vw_shared` | View only | No access | View only | No access | |

Expand one row per distinct security rule. Do not sign off the migration until every
cell is filled and green.

---

## 9. Common security defects found post-migration

| Defect | Root cause | Prevention |
|---|---|---|
| Analyst sees all rows in a notebook | SQL RLS implemented at endpoint only | Use OneLake security or physical separation |
| Direct Lake report shows unmasked data | Direct Lake on OneLake bypasses SQL DDM | Mask at write time |
| Report performance collapses after go-live | SQL RLS forced DirectQuery fallback for the whole model | Move RLS to the semantic model or OneLake |
| Shortcut query fails with a permission error | Delegated mode + OneLake rules on the source table | Switch the consumer to user identity mode, or remove source-side rules |
| Query fails for a user with correct SQL grants | Item-level Read permission missing, or item owner lacks OneLake access | Check item sharing and item ownership |
| A user in a permitted group is denied | Nested group membership not resolved across producer/consumer | Grant the exact group referenced in the producer role |
| Column reappears for some users | Additive role permissions — another role grants the column | Audit the union of all roles per principal |
