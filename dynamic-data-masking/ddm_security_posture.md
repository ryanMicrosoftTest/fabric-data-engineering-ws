# Dynamic Data Masking in Fabric — Security Posture (One Page)

**Audience:** security review / data governance
**Scope:** Fabric Warehouse and SQL analytics endpoint Dynamic Data Masking (DDM), plus the
view-and-role and Spark patterns in this folder.

---

## 1. The one-sentence position

**Dynamic Data Masking is an obfuscation control, not a security boundary.** It reduces
*accidental* exposure of sensitive values to users who are already authorised to query the
data. It does not stop a determined user with query rights from recovering the underlying
values, and it must never be the only control protecting regulated data.

Microsoft states this directly:

> "Dynamic data masking doesn't prevent database users from connecting directly to the
> database and running exhaustive queries that expose pieces of the sensitive data. Use
> dynamic data masking together with other Fabric security features like column-level
> security and row-level security to protect sensitive data in the database."
>
> — *Dynamic data masking in Fabric Data Warehouse*, Microsoft Learn

---

## 2. Why it is not a boundary — the mask leaks through predicates

The masked value is removed from the **result set**, but the **query engine still evaluates
the real value**. Any operation that reveals whether a predicate matched leaks information:

| Technique | Example | What leaks |
|---|---|---|
| Range / equality probing | `WHERE salary BETWEEN 99999 AND 100001` returns rows showing `0` | The real value is in that range |
| Binary search | Repeat the above, halving the range each time | The exact value, in ~log₂(n) queries |
| Equality on a known value | `WHERE social_security_number = '123-45-6789'` | Confirms or denies a specific value |
| Join to an unmasked source | Join the masked table to any table the user can read on the same key | Full re-identification |
| Aggregation / grouping | `GROUP BY masked_column`, `COUNT(DISTINCT …)` | Cardinality, distribution, and grouping of real values |
| Ordering | `ORDER BY masked_column` | Relative ordering of real values |

None of these require elevated permissions — only `SELECT`. A user who can query the table
can, given time, reconstruct masked values.

**Additional exposure to note:** in this folder's conditional pattern, the join key between
`employee` and `student` is the SSN itself. Any principal able to run that join sees raw
values regardless of what the masked view returns.

---

## 3. Masking does not follow the data across access paths

This is the most commonly misunderstood point. A mask is a property of the **T-SQL query
surface**, not of the stored data. The same Delta files are reachable by several paths, and
most of them do not honour the mask.

| Access path | Honours a warehouse/SQL-endpoint mask? | Control that actually applies |
|---|---|---|
| T-SQL over Warehouse / SQL analytics endpoint | **Yes** | DDM + `UNMASK` grants, OLS/CLS/RLS |
| Spark (notebook, job) reading Delta tables/files | **No** | OneLake data access roles, workspace roles |
| OneLake shortcut from another workspace or item | **No** | OneLake data access roles on source and target |
| Direct Lake semantic model | **No** — reads the Delta files, not the masked query surface | Semantic-model OLS/RLS, defined separately |
| Import / DirectQuery semantic model over the SQL endpoint | Depends on the connecting identity's effective permissions | DDM as evaluated for that identity |
| OneLake file/API download, `abfss://` access | **No** | OneLake data access roles |
| Mirroring / replication out of Fabric | **No** | Controls at the destination |

**Concrete implication for the assets in this folder:** the native mask is applied to
`dbo.employee` on the lakehouse SQL analytics endpoint, and a semantic model over the same
lakehouse sits alongside it. A user with access to the semantic model or to Spark reads the
**unmasked** Delta files. Masking one surface while leaving the others open produces a false
sense of protection.

**Rule to carry into design:** enumerate every access path to the table, then decide the
control for each. A mask on one path is not a control on the others.

---

## 4. Implicit unmask — who always sees real values

| Principal | Sees unmasked data | Why |
|---|---|---|
| Workspace **Admin**, **Member**, **Contributor** | Always | Hold `CONTROL` on the database by design, which includes `UNMASK` |
| Item owner / elevated Warehouse permissions | Always | `CONTROL` includes `UNMASK` |
| Anyone granted `UNMASK` (column, table, schema, or database scope) | Yes, within that scope | Explicit grant |
| Anyone with `ALTER ANY MASK` | Effectively yes | Can remove the mask |
| Everyone else with `SELECT` | No | Sees the mask |

Two consequences:

1. **Masking cannot be verified from an admin session.** Every test must be run as a
   non-privileged principal. A demo run by an admin proves nothing.
2. **Workspace role membership is part of the masking control.** Adding someone as a
   workspace Contributor silently unmasks every masked column in the workspace. Workspace
   role review therefore belongs in the same governance process as `UNMASK` grants.

---

## 5. Required layering for regulated data

DDM alone is insufficient. The following controls must be combined, and each solves a
different problem:

| Control | What it does | What it does **not** do |
|---|---|---|
| **Fabric workspace roles / item permissions** | Coarse access to the item; who can connect at all | Not column- or row-aware; grants implicit unmask at Admin/Member/Contributor |
| **Object-level security (OLS)** | Who can read which table/view | Nothing within a table |
| **Column-level security (CLS)** | Removes the column entirely for unauthorised users — the query **fails** rather than returning a masked value | No row-level condition |
| **Row-level security (RLS)** | Filters rows by predicate, evaluated per principal | Does not hide values in rows the user may see |
| **Dynamic Data Masking** | Obfuscates values in returned rows | Not a boundary; leaks via inference; SQL surface only |
| **OneLake data access roles** | Governs the file/folder/table layer — the Spark, shortcut, and Direct Lake paths | Does not apply masking semantics |
| **Sensitivity labels / Purview** | Classification, downstream label inheritance, DLP, audit | Not an access control by itself |

**Recommended posture for regulated columns such as SSN:**

1. **Prefer not to land the value at all** if the analytical use case does not need it —
   tokenise or hash at ingestion. Nothing beats absence.
2. **CLS or omission** for populations that must never see the column. CLS fails the query
   rather than returning a decodable placeholder, so it is the stronger control.
3. **RLS** where the restriction is row-scoped rather than column-scoped.
4. **DDM** as a convenience layer on top, to reduce accidental shoulder-surfing exposure for
   populations that are already authorised.
5. **OneLake data access roles** to close the Spark / shortcut / Direct Lake paths, so the
   SQL-surface control is not trivially bypassed.
6. **Deny the raw path.** Wherever a masked view is the intended surface, `DENY SELECT` on
   the base tables (and restrict OneLake access) so the mask cannot be walked around.
7. **Audit and review.** Log access to sensitive columns; review `UNMASK` grants and
   workspace role membership on a schedule.

---

## 6. Suitability summary

| Use case | DDM appropriate? |
|---|---|
| Reduce accidental exposure to already-authorised analysts | ✅ Yes |
| Non-production or demo data | ✅ Yes |
| Make an application's default result set less sensitive | ✅ Yes |
| Sole control protecting regulated data from a user with query rights | ❌ **No** |
| Meeting a de-identification or minimum-necessary obligation | ❌ **No** — the data is still present and inferable |
| Protecting data from Spark, shortcut, or Direct Lake consumers | ❌ **No** — those paths do not honour the mask |
| Row-conditional masking | ❌ **Not supported natively** — requires the view pattern in this folder |

---

## 7. Questions to answer before adopting DDM on a sensitive column

1. Which access paths exist for this table today — T-SQL, Spark, shortcut, semantic model,
   export? Which of them are governed?
2. Who holds workspace Admin/Member/Contributor, and is that list smaller than the list of
   people intended to see raw values?
3. Is the sensitive value needed downstream at all, or can it be tokenised at ingestion?
4. Should unauthorised users see a masked value, or should the column be invisible (CLS)?
5. Are OneLake data access roles in place, or is the Delta layer effectively open?
6. Is access to the sensitive column audited, and are `UNMASK` grants reviewed periodically?
7. Has masking been verified from a **non-privileged** session, not an admin session?

---

## References

- [Dynamic data masking in Fabric Data Warehouse](https://learn.microsoft.com/fabric/data-warehouse/dynamic-data-masking)
- [How to implement dynamic data masking in Fabric Data Warehouse](https://learn.microsoft.com/fabric/data-warehouse/howto-dynamic-data-masking)
- [Row-level security in Fabric data warehousing](https://learn.microsoft.com/fabric/data-warehouse/row-level-security)
- [Column-level security in Fabric data warehousing](https://learn.microsoft.com/fabric/data-warehouse/column-level-security)
- [Secure your Fabric data warehouse](https://learn.microsoft.com/fabric/data-warehouse/security)
- [OneLake data access roles](https://learn.microsoft.com/fabric/onelake/security/data-access-control-model)
