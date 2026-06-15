import azure.durable_functions as df
import azure.functions as func

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# Triggers
# Activities
from activities import (  # noqa: E402, F401
    collect_environments,
    collect_job_instances,
    collect_livy_sessions,
    collect_livy_statements,
    collect_notebook_definitions,
    collect_notebook_items,
    collect_spark_applications,
    collect_spark_pools,
    collect_workspace,
    merge_environment_versions,
    merge_notebook_runs,
    update_watermarks,
)

# Orchestrators
from orchestrators import (  # noqa: E402, F401
    collector_orchestrator,
    merge_orchestrator,
    workspace_sub_orchestrator,
)
from triggers import health_check, http_starter, timer_starter  # noqa: E402, F401
