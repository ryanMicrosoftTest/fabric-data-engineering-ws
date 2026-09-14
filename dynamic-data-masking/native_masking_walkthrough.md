# Native Column Masking — Walkthrough Guide (Group-Based Unmasking)

Narration guide for **Approach 2**: Fabric's native Dynamic Data Masking. A column is masked
or unmasked **in its entirety** based on the caller's permissions — there is no per-row
condition.

**Assets:** `tsql_native_ddm_ssn_nb` · `ddm-native-example.excalidraw` ·
`images/native-masking-overview.png` · `images/image-of-employee-table-from-*-user.png`

---

## 1. What this approach is

A single base table, one mask definition, no views and no duplicated data. Different
principals querying the **same table** see different results based purely on permissions.

```sql
ALTER TABLE dbo.employee
    ALTER COLUMN social_security_number
    ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)');
```

`partial(prefix, [padding], suffix)` exposes `prefix` leading characters and `suffix` trailing
characters, replacing the middle with the literal `padding`. `partial(0,"XXX-XX-",4)` exposes
no leading characters, injects `XXX-XX-`, and retains the last 4 — producing `XXX-XX-6789`,
matching the output of the conditional example.

**Everything here is product feature.** Unlike Approach 1, there is no custom code to
maintain. That is the main argument in its favour.

---

## 2. Group-based unmasking — the permission model

Grant `UNMASK` to an **Entra security group**, never to individual users. Membership is then
managed in Entra ID; no T-SQL change is required when people join or leave.

```sql
-- 1. The group must exist as a database principal
CREATE USER [<unmask-group>] FROM EXTERNAL PROVIDER;

-- 2. UNMASK does not grant read access — SELECT is still required
GRANT SELECT ON OBJECT::dbo.employee TO [<unmask-group>];

-- 3. Least privilege: unmask only the column that needs to be readable
GRANT UNMASK ON dbo.employee(social_security_number) TO [<unmask-group>];
```

**Choose the narrowest scope that works:**

| Scope | Statement | Exposure |
|---|---|---|
| **Column** | `GRANT UNMASK ON dbo.employee(social_security_number) TO [...]` | One column — **preferred** |
| Table | `GRANT UNMASK ON OBJECT::dbo.employee TO [...]` | Every masked column on that table |
| Database | `GRANT UNMASK TO [...]` | Every masked column in the database, including ones added later |

The database scope is the most common mistake: it silently unmasks columns that do not yet
exist. Prefer column scope and add grants deliberately.

**Two properties of `UNMASK` to state explicitly:**

1. It is **layered on top of `SELECT`**. It changes only how masked columns render. It does
   not grant read access and does not override a `DENY`.
2. It is **not a row filter**. Unmasking is all-or-nothing for the column. If some rows must
   be hidden, that is RLS, not DDM.

---

## 3. Suggested Entra group model

Map the pattern onto real groups before adopting it. A minimal, workable model:

| Group (rename to local convention) | Fabric / SQL grant | Sees SSN |
|---|---|---|
| `sg-fabric-<domain>-engineers` | Workspace **Contributor** (or `UNMASK` + `SELECT`) | **Yes** — note Contributor grants implicit unmask |
| `sg-fabric-<domain>-unmask` | `SELECT` + column-scoped `UNMASK` | **Yes**, by explicit grant |
| `sg-fabric-<domain>-analysts` | `SELECT` only | No — sees `XXX-XX-####` |
| `sg-fabric-<domain>-readers` | `SELECT` on curated views only | No |

**Design rules**

1. **One purpose per group.** Do not reuse an existing operational group for unmasking; the
   membership lists will diverge from intent.
2. **Nested groups work**, but review them — a nested group can quietly widen the unmask
   population. Enumerate transitive membership during review, not just direct membership.
3. **Workspace roles are part of this model.** Admin, Member, and Contributor hold `CONTROL`
   and therefore implicit `UNMASK`. The effective unmask population is
   *(explicit `UNMASK` grantees)* **∪** *(workspace Admin/Member/Contributor)*. Both lists
   must be reviewed together, or the review is meaningless.
4. **Service principals and pipeline identities count.** A pipeline running as a Contributor
   reads unmasked values and may write them to a less-protected destination.
5. **Review on a schedule** and log the review. Group membership drifts.

---

## 4. Demonstrating it — two identities, one table

| Persona | Permission | Sees | Screenshot |
|---|---|---|---|
| **Unmasked Reader** | `SELECT` + `UNMASK` on the column | Full value | `images/image-of-employee-table-from-unmasked-user.png` |
| **Masked Reader** | `SELECT` only | `XXX-XX-####` | `images/image-of-employee-table-from-masked-user.png` |
| Workspace admin / owner | Implicit `CONTROL` | Full value **always** | — |

**Critical demo caveat:** masking **cannot** be demonstrated from an admin, owner, or
`db_owner` session — those principals always see real values. A demo run entirely from the
authoring session proves nothing. Connect as a genuinely non-privileged principal (SSMS, the
VS Code MSSQL extension, or a second browser session) for the masked half of the comparison.

**Session sequence**

1. Show `dbo.employee` unmasked from the privileged session — establish the baseline.
2. Apply the mask (`ALTER TABLE ... ADD MASKED WITH`).
3. Show `sys.masked_columns` — the mask is now metadata on the column, inspectable and
   auditable.
4. Query as the **masked** principal → `XXX-XX-####`.
5. Grant column-scoped `UNMASK` to the group; re-query as that principal → full value.
6. Revoke; re-query → masked again.
7. Run the `UNMASK`-holders query from the notebook and pair it with the workspace role list
   to show the true unmask population.

---

## 5. Verification queries

```sql
-- Which columns carry a mask, and which function?
SELECT t.name AS table_name, c.name AS column_name, c.is_masked, c.masking_function
FROM sys.masked_columns AS c
JOIN sys.tables AS t ON c.object_id = t.object_id;

-- Who holds UNMASK explicitly, and at what scope?
SELECT pr.name AS principal_name, pr.type_desc, pe.state_desc,
       pe.permission_name, pe.class_desc AS permission_scope
FROM sys.database_permissions AS pe
JOIN sys.database_principals AS pr ON pe.grantee_principal_id = pr.principal_id
WHERE pe.permission_name = 'UNMASK';
```

Neither query lists workspace Admin/Member/Contributor members. **Review workspace role
membership separately** — it is the larger risk.

---

## 6. Native vs. conditional — choosing per column

| Requirement | Native DDM (Approach 2) | View + role (Approach 1) |
|---|---|---|
| Available surfaces | SQL analytics endpoint **and** Warehouse | Both |
| Mask an entire column for most users | ✅ Best fit | Possible, heavier |
| **Row-dependent masking** | ❌ Not supported | ✅ Required approach |
| Single copy of data | ✅ | ✅ |
| Custom code to maintain | **None** | View, function, and DENY set |
| Control mechanism | `GRANT UNMASK` | `GRANT SELECT` on view + `DENY` on base |
| Failure mode if misconfigured | Column renders unmasked | Base table readable — full bypass |
| Covers Spark / OneLake / Direct Lake | ❌ No | ❌ No (Spark method A does) |
| Mask visible in metadata (`sys.masked_columns`) | ✅ Auditable | ❌ Rule lives in view DDL |

**Guidance:** default to native DDM. Reach for the conditional pattern **only** when the
requirement is genuinely row-dependent, because it converts a product feature into code the
adopting team owns. Where the column must be invisible rather than obfuscated, use
column-level security instead of either — see `ddm_security_posture.md` §5.

---

## 7. Limitations to state during the walkthrough

1. **Unconditional and per column.** No row logic. If the rule is row-scoped, this is the
   wrong tool.
2. **Four mask functions only** — `default()`, `email()`, `random()` (numeric), `partial()`.
   None are conditional, none are reversible, none are user-specific.
3. **`partial()` retains real characters** by design. The last four digits are real data.
4. **Implicit unmask for privileged roles** — admins, owners, `db_owner`, and anyone with
   `CONTROL`.
5. **Not a security boundary.** Values are inferable through predicates, joins, ordering, and
   aggregation. Microsoft's own documentation says so.
6. **SQL surface only.** Spark, OneLake shortcuts, and Direct Lake semantic models read the
   underlying Delta files unmasked.

Full detail in `ddm_security_posture.md`.
