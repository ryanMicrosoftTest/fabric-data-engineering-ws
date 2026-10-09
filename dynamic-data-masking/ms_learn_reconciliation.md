# MS Learn Reconciliation — Documented DDM vs. the Examples in this Folder

Reconciles the Microsoft Learn guidance for Fabric Dynamic Data Masking against the code
delivered in this folder, and explains where the examples extend beyond documented product
behaviour.

**Articles reconciled**

| Article | Purpose |
|---|---|
| [Dynamic data masking in Fabric Data Warehouse](https://learn.microsoft.com/fabric/data-warehouse/dynamic-data-masking) | Concept: mask functions, permissions, security caveats |
| [How to implement dynamic data masking in Fabric Data Warehouse](https://learn.microsoft.com/fabric/data-warehouse/howto-dynamic-data-masking) | Step-by-step walkthrough |

---

## 1. Applicability — the article title says "Data Warehouse", the feature is broader

Both articles carry: **"Applies to: ✅ SQL analytics endpoint and Warehouse in Microsoft
Fabric."** The how-to *uses* a Warehouse for its exercise, and its prerequisites note that
"Dynamic data masking works on SQL analytics endpoint" and that masks can be added to
existing columns with `ALTER TABLE ... ALTER COLUMN`.

**Reconciliation:** the example in this folder (`tsql_native_ddm_ssn_nb`) applies the mask
over a **lakehouse SQL analytics endpoint** rather than a Warehouse item. This is supported
and is not a divergence — but it is the most likely question raised by the article's title.
If a Warehouse-item demonstration is required, the same `ALTER TABLE ... ADD MASKED WITH`
and `GRANT UNMASK` statements apply unchanged.

---

## 2. Mask functions — documented set vs. what the examples use

The concept article documents exactly **four** mask types:

| Function | Behaviour | Used in this folder? |
|---|---|---|
| `default()` | Full mask by data type — `XXXX` for strings, `0` for numerics, `1900-01-01` for dates, single zero byte for binary | No |
| `email()` | `aXXX@XXXX.com` | No |
| `random(start, end)` | Random value in range, **numeric types only** | No |
| `partial(prefix, [padding], suffix)` | Exposes leading `prefix` chars and trailing `suffix` chars, custom `padding` between | **Yes** |

The example uses:

```sql
ALTER TABLE dbo.employee
    ALTER COLUMN social_security_number
    ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)');
```

This is **identical in form to the article's own SSN example**
(`SSN CHAR(11) MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)')`), applied via `ALTER`
rather than at `CREATE TABLE` time. **No divergence.**

**Caveat worth stating:** `partial()` with a fixed literal is *format-preserving*, and the
last four digits are deliberately retained. That retained suffix is real data. Where an
identifier must not be partially recoverable, `default()` (or removing the column via CLS)
is the stronger choice.

**Known gap:** none of the four functions accepts a condition. There is no documented native
mechanism for row-dependent masking. That gap is the entire reason Approach 1 in this folder
exists.

---

## 3. Permissions — documented model vs. the examples

Documented requirements:

| Operation | Required permission |
|---|---|
| Create a table with masked columns | `CREATE TABLE` + `ALTER` on the schema |
| Add / replace / remove a mask | `ALTER ANY MASK` + `ALTER` on the table |
| View masked data | `SELECT` on the table |
| View unmasked data | `UNMASK` on the column, **or** `CONTROL` on the database |

Plus, verbatim from the article: users see masked data "if they're not members of the
Administrator, Member, or Contributor roles in the workspace, or don't have elevated
permissions on the Warehouse", and "Administrative users or roles such as Admin, Member, or
Contributor have `CONTROL` permission on the database by design and can view unmasked data
by default."

**Reconciliation and corrections applied to the examples:**

| Item | Article | Previous state of the example | Now |
|---|---|---|---|
| Grant scope | `GRANT UNMASK ON dbo.EmployeeData TO [...]` — object-scoped | Database-scoped `GRANT UNMASK TO [user]` | Column-scoped `GRANT UNMASK ON dbo.employee(social_security_number)`, with broader scopes documented as alternatives |
| Grantee | Article demonstrates both a user and a role (`TO [TestRole]`) | A single named user | An **Entra security group** created via `CREATE USER ... FROM EXTERNAL PROVIDER` |
| `SELECT` prerequisite | Implied — the test user already queries the table | Not stated | Explicit `GRANT SELECT` alongside the `UNMASK` grant |
| Verification identity | Article is explicit: test as a user *without* Admin/Member/Contributor | Not stated | Called out in the notebook, with a query listing current `UNMASK` holders |

**Behavioural clarification, not documented in one place:** `UNMASK` is layered on top of
`SELECT`. It changes only how masked columns render — it does not grant read access and does
not override a `DENY`. A principal must already be able to `SELECT` before `UNMASK` has any
effect.

---

## 4. Security caveats — the article agrees with this folder's position

The concept article contains an explicit section, *"Security consideration: bypassing masking
by using inference or brute-force techniques"*, with a worked salary-range example, and
concludes:

> "Don't use dynamic data masking alone to fully secure sensitive data from users with query
> access to the Warehouse or SQL analytics endpoint. Dynamic data masking is appropriate for
> preventing accidental sensitive data exposure, but it doesn't protect against malicious
> intent to infer the underlying data."

It also directs readers to combine DDM with column-level and row-level security.

**Reconciliation:** the position in `ddm_security_posture.md` is Microsoft's own documented
position, not an added opinion. `ddm_security_posture.md` extends it in one area the article
does **not** cover: masking behaviour across **non-T-SQL access paths** (Spark, OneLake
shortcuts, Direct Lake semantic models). The article scopes itself to the SQL surface and is
silent on the rest, which is a frequent source of misunderstanding.

---

## 5. Where the delivered examples extend beyond the product

| Capability | Native DDM | This folder | Ownership |
|---|---|---|---|
| Unconditional column mask | ✅ Product feature | `tsql_native_ddm_ssn_nb` | Microsoft |
| Choice of four mask functions | ✅ Product feature | `partial()` used | Microsoft |
| `UNMASK` grant model | ✅ Product feature | Group-based grants | Microsoft |
| **Row-conditional masking** | ❌ Not available | `tsql_data_mask_ssn_nb` — view + role + `DENY` | **Customer-owned custom code** |
| **Masking baked into persisted data** | ❌ Not applicable | `data_masking_spark_files_nb` — Spark transformation | **Customer-owned custom code** |
| Coverage of Spark / OneLake / Direct Lake paths | ❌ Out of scope | Not solved by any pattern here — requires OneLake data access roles | Customer-owned design |

The distinction matters operationally: everything in the "customer-owned" rows must be
maintained, tested, and regression-checked by the adopting team. Product features do not
carry that burden.

---

## 6. Divergences and gaps — summary

| # | Finding | Severity | Status |
|---|---|---|---|
| 1 | Example applies the mask on a lakehouse SQL analytics endpoint; the how-to article uses a Warehouse item | Informational | Documented above; behaviour is identical |
| 2 | Example previously granted `UNMASK` at database scope to a named user; the article demonstrates object-scoped grants and role grants | Medium | **Fixed** — column-scoped, group-based |
| 3 | Example did not grant `SELECT` explicitly alongside `UNMASK` | Low | **Fixed** |
| 4 | Example did not state that masking is unverifiable from an admin session | Medium | **Fixed** — noted in both notebooks and the posture doc |
| 5 | Conditional masking is not a documented product capability | High (expectation-setting) | Documented as custom code in `conditional_masking_walkthrough.md` |
| 6 | Neither article addresses masking behaviour on Spark / OneLake / Direct Lake paths | High | Covered in `ddm_security_posture.md` §3 |
| 7 | `partial()` retains real trailing characters by design | Medium | Called out in §2 above |

---

## 7. Third-party and community articles

Community write-ups on Fabric DDM generally reproduce the same `ALTER TABLE ... ADD MASKED
WITH` and `GRANT UNMASK` mechanics. When evaluating any such article, check three things
before adopting its guidance:

1. **Does it distinguish `UNMASK` scope** (column / table / schema / database)? Many examples
   use the broadest scope for convenience.
2. **Does it acknowledge implicit unmask** for workspace Admin/Member/Contributor? If not,
   its verification steps are unreliable.
3. **Does it claim masking protects data from all consumers?** If it does not scope the claim
   to the T-SQL surface, that claim is wrong for Spark, shortcuts, and Direct Lake.

Anything that passes those three checks is consistent with the material in this folder.

---

## 8. Talking points

1. Native DDM does exactly what the article says: four functions, unconditional, per column,
   `UNMASK`-gated. The example matches it — the SSN mask string is the article's own.
2. The article's title says "Data Warehouse", but the feature applies to the SQL analytics
   endpoint too. The example uses the endpoint.
3. There is no native conditional masking. The view/role pattern here is custom code that
   the adopting team owns and maintains.
4. Microsoft's own documentation states DDM is not a boundary and must be layered with CLS
   and RLS. This is not a caveat added here.
5. The documentation is silent on non-SQL access paths. That silence is the real risk:
   masking the SQL endpoint while Spark, shortcuts, and Direct Lake read the same files
   unmasked.
6. Choose per column: CLS if the column must be invisible, RLS if the restriction is
   row-scoped, DDM only as a convenience layer on top.
