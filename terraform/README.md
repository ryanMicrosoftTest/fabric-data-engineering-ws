# Terraform — cross-workspace sharing via a delegated SQL analytics endpoint

Provisions a **consumer** Fabric workspace that exposes a filtered slice of a **producer**
lakehouse through curated SQL views, where the consuming principal holds **no permission
whatsoever on the producer**.

The whole security boundary is T-SQL `GRANT`/`DENY`, which is what makes it usable for
teams that want classic DBA-style control rather than OneLake security roles.

## The design in one paragraph

The consumer lakehouse's SQL analytics endpoint runs in **delegated identity mode**, so it
reads OneLake as the item's **owning identity**, not the caller's. A **passthrough** shortcut
surfaces the producer schema. Curated `dbo` views select from that schema, and ownership
chaining carries the read through under the owner. The consuming principal therefore holds
**no producer-side permission at all** — only `GRANT SELECT` on the views, plus a
`DENY SELECT ON SCHEMA::` over the shortcut schema for containment and metadata hiding.

This has been validated end to end against a non-privileged principal: the views returned
the filtered slice, the base tables were denied, and the shortcut schema was invisible in
Object Explorer.

## Layout

| File | Purpose |
| --- | --- |
| `providers.tf` | Provider pinning and backend. `preview = true` is required for the OneLake security resource. |
| `variables.tf` | Inputs, with validations encoding the constraints that make the design safe. |
| `main.tf` | Workspace, role assignment, lakehouse, shortcut, optional producer role. |
| `sql.tf` | Renders and applies the T-SQL layer via `local-exec`. |
| `outputs.tf` | Endpoint coordinates plus the post-deploy checklist. |
| `envs/*.tfvars` | Per-environment variable values, committed. |
| `sql/*.tftpl` | Templated SQL. Edit these, never `.rendered/`. |
| `scripts/Invoke-FabricSql.ps1` | Entra-token SQL runner. |
| `.rendered/` | The exact SQL that was applied. Reviewable, diffable, gitignored. |

---

# Step-by-step: deploy into a brand-new workspace

This walks the full path for a fresh validation run. The module **creates** the consumer
workspace, so nothing needs to be pre-created on the consumer side.

## Step 0 — Prerequisites

```powershell
# Terraform 1.6+
terraform version

# PowerShell 7 + the SqlServer module (used to apply the T-SQL layer)
$PSVersionTable.PSVersion
Install-Module SqlServer -Scope CurrentUser

# Azure CLI, signed in as the identity that will OWN the consumer lakehouse
az login
az account show --query user.name -o tsv
```

**The identity you are signed in as matters more than anything else here.** In delegated
identity mode the SQL endpoint impersonates the lakehouse **owner** when reading OneLake.
Whoever runs `terraform apply` becomes that owner, so **that identity must have read access
to the producer data**. If it does not, the views are created successfully and then fail at
query time.

That identity also needs permission to create workspaces and assign them to a capacity.

## Step 1 — Collect the values you need

**Capacity ID** — the consumer workspace is attached to it:

```powershell
az rest --method get `
  --url "https://api.fabric.microsoft.com/v1/capacities" `
  --resource "https://api.fabric.microsoft.com" `
  --query "value[].{name:displayName,id:id,sku:sku,state:state}" -o table
```

**Producer workspace and lakehouse IDs** — easiest from the portal URL with the producer
lakehouse open:

```
https://app.fabric.microsoft.com/groups/<producer_workspace_id>/lakehouses/<producer_lakehouse_id>
```

Or list them:

```powershell
az rest --method get `
  --url "https://api.fabric.microsoft.com/v1/workspaces" `
  --resource "https://api.fabric.microsoft.com" `
  --query "value[].{name:displayName,id:id}" -o table

az rest --method get `
  --url "https://api.fabric.microsoft.com/v1/workspaces/<producer_workspace_id>/lakehouses" `
  --resource "https://api.fabric.microsoft.com" `
  --query "value[].{name:displayName,id:id}" -o table
```

**Producer schema path.** The producer lakehouse must be **schema-enabled**; the shortcut
targets a schema, e.g. `Tables/health_dbo`. Confirm the schema name in the lakehouse
Explorer under `Tables`.

**Consuming principal** — the user who will only ever see the views:

```powershell
az ad user show --id "<user>@<tenant>.onmicrosoft.com" --query "{id:id,upn:userPrincipalName}" -o table
```

Use the returned `id` for `consumer_principal_object_id` and the exact `userPrincipalName`
for `consumer_principal_upn`. For a B2B guest the UPN is the mangled in-tenant form
(`user_contoso.com#EXT#@<yourtenant>.onmicrosoft.com`), **not** their home-tenant address —
`CREATE USER ... FROM EXTERNAL PROVIDER` fails otherwise.

**Base table and filter.** Pick the table inside the producer schema and the row predicate
that defines the permitted slice.

## Step 2 — Write your tfvars file

Copy an existing env file and edit it. For a throwaway validation run:

```powershell
cd terraform
Copy-Item envs/prod.tfvars envs/validate.tfvars
```

Then fill it in:

```hcl
tenant_id   = "<your tenant guid>"
capacity_id = "<capacity guid from step 1>"

# Producer — referenced, never modified
producer_workspace_id = "<producer workspace guid>"
producer_lakehouse_id = "<producer lakehouse guid>"
producer_schema_path  = "Tables/<producer_schema>"

# Consumer — created by Terraform. Use a fresh, unused workspace name.
consumer_workspace_name = "delegated-sharing-validate"
consumer_lakehouse_name = "consumer_lh_validate"

# Schema the shortcut lands under, and the target of the DENY. Must not be "dbo".
consumer_schema_name = "producer_pt"

consumer_principal_object_id = "<object id from step 1>"
consumer_principal_upn       = "<exact UPN from step 1>"
consumer_principal_type      = "User"
consumer_workspace_role      = "Viewer"

# The entire permitted surface. Anything not listed here is unreachable.
views = [
  {
    name         = "v_neurology_doctors"
    source_table = "doctor_table"
    where        = "department = 'Neurology'"
  }
]

manage_producer_role = false

# See "The attestation flag" below before changing this.
endpoint_delegated_mode_confirmed = true
apply_sql                         = true
```

### Variable reference

| Variable | Notes |
| --- | --- |
| `tenant_id` | Entra tenant hosting both workspaces. |
| `capacity_id` | Fabric capacity for the consumer workspace. |
| `producer_workspace_id` / `producer_lakehouse_id` | Existing producer. Not modified unless `manage_producer_role = true`. |
| `producer_schema_path` | Must start with `Tables/`. A `Files/` shortcut is not queryable through T-SQL. |
| `consumer_workspace_name` | Must not collide with an existing workspace, or create fails. |
| `consumer_schema_name` | **Must not be `dbo`.** A `DENY` on the schema holding the views would override the `GRANT`. Enforced by a validation. |
| `consumer_principal_upn` | Used verbatim in `CREATE USER ... FROM EXTERNAL PROVIDER`. |
| `consumer_workspace_role` | Leave at `Viewer`. Contributor and above may bypass the SQL permission model, making a passing test meaningless. |
| `views` | Each entry becomes a `dbo` view plus a `GRANT SELECT`. `columns` defaults to `*`; set it for column-level restriction. |
| `manage_producer_role` | Leave `false` when the producer already grants your deploy identity access. |
| `apply_sql` | `false` renders the SQL without executing it, if you would rather apply it by hand. |

### The attestation flag

`endpoint_delegated_mode_confirmed` must be `true` or the plan fails. It is not a probe —
Terraform cannot read the endpoint mode. It is you asserting that the endpoint will stay in
**Delegated identity** mode.

Delegated identity is the **default for a new lakehouse**, so on a green-field deploy the
correct action is to set this `true` up front and then simply *not touch* the endpoint's
Security tab. You re-verify it in Step 5.

## Step 3 — Initialise

For a local validation run, drop the remote backend:

```powershell
terraform init -backend=false
```

For a real deployment with shared state:

```powershell
terraform init `
  -backend-config="resource_group_name=<tfstate-rg>" `
  -backend-config="storage_account_name=<tfstate-sa>" `
  -backend-config="container_name=tfstate" `
  -backend-config="key=fabric-delegated-onelake/validate.tfstate" `
  -backend-config="use_azuread_auth=true"
```

## Step 4 — Plan and apply

```powershell
terraform plan -var-file=envs/validate.tfvars
```

Expect 7–8 resources: workspace, role assignment, lakehouse, shortcut, three `local_file`,
and the two `null_resource` provisioners.

```powershell
terraform apply -var-file=envs/validate.tfvars
```

**If the apply fails on `Invalid object name`,** the SQL endpoint has not yet caught up with
the shortcut — it lags creation by up to ~2 minutes. Wait, then re-run `terraform apply`.
Everything in the SQL layer is idempotent.

On success `null_resource.verify` prints:

```
OK - structure, permissions and ownership chain verified.
```

That confirms the objects, the permission shape and the ownership chain. It does **not**
confirm the design works — see Step 6.

## Step 5 — Confirm what Terraform could not

```powershell
terraform output post_deploy_checklist
```

1. **Endpoint mode.** Consumer lakehouse → SQL analytics endpoint → Settings → Security.
   It must read **Delegated identity**, not "Use OneLake security for tables". If it is in
   User identity mode, stop — ownership chaining is disabled and every grant is inert.
2. **No OneLake security roles on the consumer lakehouse.** Manage OneLake data access →
   the list must be empty. Any role there displaces the SQL permission model.

## Step 6 — Validate as the consuming principal

**This is the only step that actually proves the design.** Everything before it ran as you,
and you are privileged — a passing `verify.sql` cannot distinguish "ownership chaining
resolved under the delegated owner" (working) from "resolved under a privileged caller"
(leaking).

```powershell
terraform output -raw sql_endpoint_server   # connect here
Get-Content .rendered/consumer_probe.sql
```

Sign in **as the consuming principal** (SSMS, Azure Data Studio, or the portal SQL editor)
and run `.rendered/consumer_probe.sql`. Expected results:

| Query | Expected |
| --- | --- |
| `SUSER_NAME()` | The consuming principal, not you. |
| `INFORMATION_SCHEMA.TABLES` | Only `dbo` views. The shortcut schema must be **invisible**. |
| `SELECT * FROM dbo.<view>` | The filtered slice only. |
| `SELECT COUNT(*) FROM <shortcut_schema>.<table>` | **Permission denied** (Msg 373 / external policy action). |

Any deviation is a real finding, not a setup error:

- Views denied → ownership chaining did not traverse the shortcut. Re-check Step 5 item 1.
- Views return unfiltered rows → the predicate is not being applied.
- Base tables readable → the `DENY` is not containing the user; the pattern is unsafe as drawn.

## Step 7 — Tear down

```powershell
terraform destroy -var-file=envs/validate.tfvars
```

The producer is untouched by destroy unless `manage_producer_role = true`.

---

## What Terraform cannot do here — read this before trusting a green apply

**1. The SQL endpoint access mode is unmanageable.**
There is no provider resource and no documented public API for delegated vs user identity
mode. It is also the single highest-risk setting: in **user identity mode** the security sync
service explicitly disables ownership chaining, every `GRANT` in `sql/` becomes inert, and the
views fail until you grant the base tables — which defeats the entire design. This is the most
common cause of the pattern appearing not to work.

The module handles this the only honest way available: `endpoint_delegated_mode_confirmed`
must be explicitly set to `true`, as an operator attestation.

**2. The absence of OneLake security on the consumer is load-bearing.**
There is deliberately no `fabric_onelake_data_access_security` resource targeting the consumer
lakehouse. Terraform cannot assert a negative against drift — if someone adds a OneLake role
later, the SQL permission model is displaced for those objects and this config will still plan
clean. Re-check Step 5 item 2 after any manual change.

**3. Verification runs as the deployer, not the consumer.**
`sql/verify.sql` checks structure, permission shape and ownership chain, but the deploying
identity is privileged and typically owns the lakehouse. Only Step 6 settles it. Treat the
deployment as unvalidated until the consumer probe has been run.

## Design constraints encoded as guardrails

| Guardrail | Why |
| --- | --- |
| `consumer_schema_name` may not be `dbo` | A `DENY` on the schema holding the views would override the `GRANT` and break everything. |
| `producer_schema_path` must start with `Tables/` | The design shortcuts a schema; a `Files/` shortcut is not queryable through T-SQL. |
| Only `target.onelake` is used for the shortcut | The provider's OneLake target exposes no `connection_id`, so it can only produce a **passthrough** shortcut. A delegated/SPN shortcut is never enumerated by a delegated-mode endpoint. The misconfiguration is unrepresentable. |
| `consumer_workspace_role` defaults to `Viewer` | Contributor and above may bypass the SQL permission model, making a positive test meaningless. |
| `producer_role_object_id` documented as the lakehouse *owner* | In delegated mode the producer evaluates the owning identity, not the consuming user. Naming the consumer here would be a silent no-op that looks correct. |

## Known rough edges

- **First apply can fail on "invalid object name".** The SQL endpoint lags shortcut creation by
  up to ~2 minutes. Everything in `delegated_access.sql` is idempotent — re-run `terraform apply`.
- **`null_resource.verify` runs on every apply** (`timestamp()` trigger). Intentional: the
  assertions are cheap and drift here is silent.
- **`DROP VIEW` / `CREATE VIEW` on every SQL change.** Grants are reapplied in the same script,
  so there is no permission gap in steady state, but the view is briefly absent mid-apply.
- **`local-exec` means this needs a runner with `pwsh`, the `SqlServer` module and an
  authenticated `az` CLI.** A hosted CI agent will not have these by default, and the
  identity it authenticates as becomes the lakehouse owner — see Step 0.

## Adopting an existing environment instead of creating a new one

To bring already-provisioned objects under management rather than duplicating them:

```powershell
terraform import fabric_workspace.consumer       "<workspaceId>"
terraform import fabric_lakehouse.consumer       "<workspaceId>/<lakehouseId>"
terraform import fabric_shortcut.producer_schema "<workspaceId>/<lakehouseId>/Tables/<shortcut_schema>"

# The role assignment ID is the assignment GUID, not the principal object ID:
terraform import fabric_workspace_role_assignment.consumer_principal "<workspaceId>/<roleAssignmentId>"
```

If the target environment also carries a **delegated (SPN-backed)** OneLake shortcut kept
as a test control, it is intentionally not modelled here — it is a negative result, not
part of the design. Remove it before treating the environment as a reference build.
