from __future__ import annotations

import logging
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))
sys.path.insert(0, str(ROOT / "plugins" / "src"))


try:
    import airflow  # noqa: F401
except ImportError:
    airflow = types.ModuleType("airflow")
    exceptions = types.ModuleType("airflow.exceptions")
    hooks = types.ModuleType("airflow.hooks")
    hooks_base = types.ModuleType("airflow.hooks.base")
    models = types.ModuleType("airflow.models")
    baseoperator = types.ModuleType("airflow.models.baseoperator")

    class AirflowException(Exception):
        pass

    class BaseHook:
        log = logging.getLogger("airflow.test")

        @classmethod
        def get_connection(cls, conn_id):
            raise NotImplementedError(conn_id)

    class BaseOperator:
        log = logging.getLogger("airflow.test")
        deferrable = False

        def __init__(self, *, task_id=None, execution_timeout=None, **kwargs):
            self.task_id = task_id
            self.execution_timeout = execution_timeout
            self.downstream_task_ids = set()
            if DAG._active is not None:
                DAG._active.task_dict[task_id] = self

        def __rshift__(self, other):
            self.downstream_task_ids.add(other.task_id)
            return other

    class DAG:
        _active = None

        def __init__(
            self,
            dag_id,
            schedule_interval=None,
            catchup=None,
            default_args=None,
            **kwargs,
        ):
            self.dag_id = dag_id
            self.schedule_interval = schedule_interval
            self.catchup = catchup
            self.default_args = default_args or {}
            self.task_dict = {}
            for key, value in kwargs.items():
                setattr(self, key, value)

        def __enter__(self):
            type(self)._active = self
            return self

        def __exit__(self, *args):
            type(self)._active = None

        def get_task(self, task_id):
            return self.task_dict[task_id]

    airflow.DAG = DAG
    exceptions.AirflowException = AirflowException
    hooks_base.BaseHook = BaseHook
    baseoperator.BaseOperator = BaseOperator
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.exceptions": exceptions,
            "airflow.hooks": hooks,
            "airflow.hooks.base": hooks_base,
            "airflow.models": models,
            "airflow.models.baseoperator": baseoperator,
        }
    )
