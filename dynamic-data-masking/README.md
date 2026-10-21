# Dynamic Data Masking in Microsoft Fabric

Two working patterns for masking sensitive column values in Fabric, with the security
analysis needed to decide whether either is appropriate.

> **Read this first:** Dynamic Data Masking is an **obfuscation** control, not a security
> boundary, and a mask applied at the SQL surface does **not** apply to Spark, OneLake
> shortcuts, or Direct Lake semantic models. See
> **[`ddm_security_posture.md`](ddm_security_posture.md)** before adopting anything here for
> regulated data.

---

## Contents

| File | What it is |
|---|---|
| [`ddm_example_overview.md`](ddm_example_overview.md) | Technical overview of both approaches and how they work |
| [`ddm_security_posture.md`](ddm_security_posture.md) | **Security posture one-pager** — limitations, access-path coverage, required layering with RLS/CLS/OneLake |
| [`ms_learn_reconciliation.md`](ms_learn_reconciliation.md) | Documented product behaviour vs. the code here; product feature vs. custom code |
| [`conditional_masking_walkthrough.md`](conditional_masking_walkthrough.md) | Walkthrough guide for Approach 1, incl. performance and maintainability analysis |
| [`native_masking_walkthrough.md`](native_masking_walkthrough.md) | Walkthrough guide for Approach 2, incl. the Entra group model |
| `data_masking_spark_files_nb.Notebook` | **Approach 1a** — conditional masking applied in Spark |
| `tsql_data_mask_ssn_nb.Notebook` | **Approach 1b** — conditional masking via view + role + `DENY` |
| `tsql_native_ddm_ssn_nb.Notebook` | **Approach 2** — native DDM (`ADD MASKED WITH` + `GRANT UNMASK`) |
| `data-masking-fabric.excalidraw` | Conditional masking scenario diagram |
| `ddm-native-example.excalidraw` | Native DDM scenario diagram |
| `images/` | Diagram exports and masked/unmasked result screenshots |
| `parameter.yml` | Optional `fabric-cicd` environment rebinding for the hardcoded item IDs |

---

## The two approaches at a glance

| | **Approach 1 — Conditional** | **Approach 2 — Native DDM** |
|---|---|---|
| Masking rule | Row-dependent (mask only the matching subset) | Unconditional, whole column |
| Native Fabric feature? | **No** — custom views/roles or Spark | **Yes** |
| Code you maintain | View, function, `DENY` set / Spark logic | None |
| Control mechanism | `GRANT SELECT` on view + `DENY` on base tables | `GRANT UNMASK` |
| Use when | The requirement is genuinely row-scoped | Anything else |

Full comparison: [`native_masking_walkthrough.md`](native_masking_walkthrough.md) §6.

---

## Prerequisites

1. A Fabric workspace on an active capacity or trial capacity.
2. A **Warehouse** or a **Lakehouse with its SQL analytics endpoint**. Native DDM applies to
   both. The notebooks here target a lakehouse SQL analytics endpoint.
3. Source tables `dbo.employee` and `dbo.student`, each with `id`, `first_name`, `last_name`,
   and `social_security_number`. Use **synthetic data only** — never load real sensitive data
   into a demo environment.
4. **Two identities** for testing:
   - an **administrative** identity (workspace Admin / Member / Contributor, or elevated
     Warehouse permissions) to apply the mask, and
   - a **non-privileged** identity to observe it.

   Masking **cannot be verified from an administrative session** — admins, the item owner, and
   anyone with `CONTROL` hold implicit `UNMASK` and always see real values.
5. An **Entra security group** for the privileged (unmask) population, and optionally one for
   the masked population. Grant to groups, not individuals.
6. Spark compute, only if running the Spark notebook.

---

## Configuration — placeholders to replace

Every environment-specific value is a placeholder. Search for `<` to find them all.

### Notebook placeholders

| Placeholder | Where | Meaning |
|---|---|---|
| `<unmask-group>` | `tsql_native_ddm_ssn_nb` | Entra security group allowed to see unmasked values |
| `<masked-readers-group>` | `tsql_data_mask_ssn_nb` | Entra security group that must see masked values |
| `<workspace-id>` | `data_masking_spark_files_nb` | Fabric workspace ID |
| `<silver-lakehouse-id>` | `data_masking_spark_files_nb` | Silver lakehouse item ID |
| `<bronze-lakehouse-id>` | `data_masking_spark_files_nb` | Bronze lakehouse item ID |

**Finding the IDs** — from the Fabric portal URL:

```
https://app.fabric.microsoft.com/groups/<workspace-id>/lakehouses/<lakehouse-id>
```

### Notebook attachment metadata

Each `.Notebook/notebook-content.*` file carries a `# META` / `-- META` block pinning the
default lakehouse and warehouse by ID. These are **environment-specific** and will not match
your tenant.

- **Running interactively:** open the notebook in Fabric and re-attach the default lakehouse /
  warehouse through the UI. The metadata rewrites itself on save.
- **Deploying with `fabric-cicd`:** use [`parameter.yml`](parameter.yml) in this folder to
  rebind the IDs per environment. Fill in your per-environment GUIDs before deploying.

---

## Running the examples

### Approach 2 — native DDM (start here; it is the simpler pattern)

1. Open `tsql_native_ddm_ssn_nb` and attach it to your warehouse / SQL analytics endpoint.
2. Replace `<unmask-group>`.
3. Run top to bottom as the **administrative** identity:
   - §1 applies `ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)')`
   - §2 confirms the mask via `sys.masked_columns`
   - §3 creates the group principal and grants column-scoped `UNMASK`
   - §4 lists current `UNMASK` holders
4. Connect as the **non-privileged** identity and run
   `SELECT TOP 20 ... FROM dbo.employee`. Expect `XXX-XX-####`.
5. Add that identity to `<unmask-group>`, re-run, and expect the full value.
6. §5 rolls everything back.

### Approach 1b — conditional masking via view + role

1. Open `tsql_data_mask_ssn_nb` and attach it to the same endpoint.
2. Replace `<masked-readers-group>`.
3. Run top to bottom as the **administrative** identity. This creates the `sec` schema,
   `sec.fn_mask_ssn`, `sec.vw_employee_masked`, and the `maskedReaders` role with its
   `GRANT`/`DENY` set.
4. §6 verifies role membership and effective permissions.
5. Connect as a member of `<masked-readers-group>` and confirm:
   - `SELECT * FROM sec.vw_employee_masked` **succeeds**, with masked values only for
     employees who are also students
   - `SELECT * FROM dbo.employee` **fails**
   - `SELECT * FROM dbo.student` **fails**
6. §7 rolls everything back.

### Approach 1a — conditional masking in Spark

1. Open `data_masking_spark_files_nb`.
2. Set `WORKSPACE_ID`, `SILVER_LAKEHOUSE_ID`, and `BRONZE_LAKEHOUSE_ID` in the configuration
   cell.
3. Run top to bottom. Note the `.isin()` scale caveat in
   [`conditional_masking_walkthrough.md`](conditional_masking_walkthrough.md) §2 before using
   this shape on production volumes.

---

## Verifying that masking actually holds

Run all five checks as a **non-privileged** principal. Checks 4 and 5 are the ones that
commonly fail — that is the point of running them.

| # | Check | Expected |
|---|---|---|
| 1 | Query the masked table or view over T-SQL | Masked values |
| 2 | Query the base tables directly | Permission error (Approach 1) / masked values (Approach 2) |
| 3 | List `UNMASK` holders **and** workspace Admin/Member/Contributor members | Both lists match the intended privileged population |
| 4 | Read the same Delta table from a Spark notebook | Should be blocked by OneLake data access roles — **otherwise the mask is bypassable** |
| 5 | Open a Direct Lake semantic model over the same lakehouse | Should not expose raw values — **otherwise the mask is bypassable** |

---

## Running this outside the authoring workspace

- The notebooks are stored in `fabric-cicd` (`.Notebook` folder) format. They can be uploaded
  to Fabric directly or deployed with `fabric-cicd` — the framework is **not required** to run
  them interactively.
- No external packages are needed. The Spark notebook uses only `pyspark.sql.functions`.
- All identity references are placeholders; no tenant-specific principal is embedded.
- Item IDs in notebook metadata must be rebound to your environment (see *Configuration*).

---

## Data and privacy note

All data, screenshots, and diagrams in this folder use **synthetic values**. No real personal
data, tenant identifiers, or customer references are included. Keep it that way — do not
commit screenshots containing real values, real principal names, or tenant identifiers.

---

## References

- [Dynamic data masking in Fabric Data Warehouse](https://learn.microsoft.com/fabric/data-warehouse/dynamic-data-masking)
- [How to implement dynamic data masking in Fabric Data Warehouse](https://learn.microsoft.com/fabric/data-warehouse/howto-dynamic-data-masking)
- [Row-level security](https://learn.microsoft.com/fabric/data-warehouse/row-level-security) · [Column-level security](https://learn.microsoft.com/fabric/data-warehouse/column-level-security)
- [Secure your Fabric data warehouse](https://learn.microsoft.com/fabric/data-warehouse/security)
- [OneLake data access roles](https://learn.microsoft.com/fabric/onelake/security/data-access-control-model)
