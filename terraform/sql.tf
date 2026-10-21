############################################
# T-SQL layer
#
# Terraform has no Fabric resource for database users, views or GRANT/DENY, so
# this is applied out-of-band via local-exec. Rendered scripts are written to
# .rendered/ so the exact SQL that ran is reviewable and diffable.
############################################

locals {
  sql_server   = fabric_lakehouse.consumer.properties.sql_endpoint_properties.connection_string
  sql_database = fabric_lakehouse.consumer.display_name

  # Quoted, comma-separated view names for the IN (...) predicates in verify.sql.
  view_name_list = join(", ", [for v in var.views : "N'${v.name}'"])

  sql_template_vars = {
    server          = local.sql_server
    database        = local.sql_database
    shortcut_schema = var.consumer_schema_name
    consumer_upn    = var.consumer_principal_upn
    views           = var.views
    view_name_list  = local.view_name_list
  }

  delegated_access_sql = templatefile("${path.module}/sql/delegated_access.sql.tftpl", local.sql_template_vars)
  verify_sql           = templatefile("${path.module}/sql/verify.sql.tftpl", local.sql_template_vars)
  consumer_probe_sql   = templatefile("${path.module}/sql/consumer_probe.sql.tftpl", local.sql_template_vars)

  rendered_dir = "${path.module}/.rendered"
}

resource "local_file" "delegated_access_sql" {
  filename        = "${local.rendered_dir}/delegated_access.sql"
  content         = local.delegated_access_sql
  file_permission = "0640"
}

resource "local_file" "verify_sql" {
  filename        = "${local.rendered_dir}/verify.sql"
  content         = local.verify_sql
  file_permission = "0640"
}

# Not executed by Terraform — it must be run interactively as the consuming
# principal. Rendered here so the operator has the exact script to hand.
resource "local_file" "consumer_probe_sql" {
  filename        = "${local.rendered_dir}/consumer_probe.sql"
  content         = local.consumer_probe_sql
  file_permission = "0640"
}

############################################
# Apply
#
# depends_on the shortcut because the views resolve through it. Note the SQL
# endpoint lags shortcut creation by up to ~2 minutes, so the first apply after
# a green-field create can fail on "invalid object name". Re-running is safe —
# everything in the script is idempotent.
############################################

resource "null_resource" "sql" {
  count = var.apply_sql ? 1 : 0

  triggers = {
    sql_sha256   = sha256(local.delegated_access_sql)
    endpoint     = local.sql_server
    database     = local.sql_database
    shortcut_id  = fabric_shortcut.producer_schema.id
    consumer_upn = var.consumer_principal_upn
  }

  depends_on = [
    fabric_shortcut.producer_schema,
    fabric_workspace_role_assignment.consumer_principal,
    local_file.delegated_access_sql,
  ]

  provisioner "local-exec" {
    interpreter = ["pwsh", "-NoProfile", "-NonInteractive", "-Command"]
    command = join(" ", [
      "& '${abspath("${path.module}/scripts/Invoke-FabricSql.ps1")}'",
      "-Server '${local.sql_server}'",
      "-Database '${local.sql_database}'",
      "-ScriptPath '${abspath("${local.rendered_dir}/delegated_access.sql")}'",
    ])
  }
}

resource "null_resource" "verify" {
  count = var.apply_sql ? 1 : 0

  triggers = {
    always = timestamp()
  }

  depends_on = [
    null_resource.sql,
    local_file.verify_sql,
  ]

  provisioner "local-exec" {
    interpreter = ["pwsh", "-NoProfile", "-NonInteractive", "-Command"]
    command = join(" ", [
      "& '${abspath("${path.module}/scripts/Invoke-FabricSql.ps1")}'",
      "-Server '${local.sql_server}'",
      "-Database '${local.sql_database}'",
      "-ScriptPath '${abspath("${local.rendered_dir}/verify.sql")}'",
    ])
  }
}
