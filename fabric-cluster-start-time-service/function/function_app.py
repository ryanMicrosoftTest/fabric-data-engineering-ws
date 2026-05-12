import azure.functions as func
import azure.durable_functions as df

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# Triggers
from triggers import timer_starter, http_starter, health_check  # noqa: E402, F401

# Orchestrators
from orchestrators import (  # noqa: E402, F401
    collector_orchestrator,
    workspace_sub_orchestrator,
    merge_orchestrator,
)

# Activities
from activities import (  # noqa: E402, F401
    collect_workspace,
    collect_notebook_items,
    collect_notebook_definitions,
    collect_environments,
    collect_spark_pools,
    collect_spark_applications,
    collect_job_instances,
    collect_livy_sessions,
    collect_livy_statements,
    merge_notebook_runs,
    merge_environment_versions,
    update_watermarks,
)
