"""
Adds helpers/plugins to sys.path and stubs Airflow imports so DAG modules
can be imported in unit tests without a running Airflow database.
"""

import os
import sys
from unittest.mock import MagicMock

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PLUGINS = os.path.join(_REPO, "airflow_lappis", "plugins")
_HELPERS = os.path.join(_REPO, "airflow_lappis", "helpers")

for _p in (_PLUGINS, _HELPERS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Stub Airflow and cloud SDKs so modules can be imported without side-effects.
_STUBS = [
    "airflow",
    "airflow.decorators",
    "airflow.sensors",
    "airflow.sensors.external_task",
    "airflow.providers",
    "airflow.providers.postgres",
    "airflow.providers.postgres.hooks",
    "airflow.providers.postgres.hooks.postgres",
    "postgres_helpers",
    "schedule_loader",
    "s3fs",
    "adlfs",
    "fsspec",
]

for _mod in _STUBS:
    sys.modules.setdefault(_mod, MagicMock())
