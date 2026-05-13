from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG


# Airflow reads this flag from the custom logging module during initialization.
REMOTE_TASK_LOG = False

# Start from Airflow's own default logging structure so required handlers such as
# "task" remain consistent with what Airflow validates during import.
LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

# The default Airflow formatters reference Airflow internals during import,
# which breaks in the isolated virtualenv used by this task. Replace only the
# formatter classes while preserving the expected formatter names.
simple_formatter = {
    "format": "[%(asctime)s] %(levelname)s - %(message)s",
    "class": "logging.Formatter",
}

for formatter_name in ("airflow", "airflow_coloured", "source_processor"):
    if formatter_name in LOGGING_CONFIG.get("formatters", {}):
        LOGGING_CONFIG["formatters"][formatter_name] = simple_formatter

simple_filter = {"()": "logging.Filter"}

for filter_name in ("mask_secrets", "mask_secrets_core"):
    if filter_name in LOGGING_CONFIG.get("filters", {}):
        LOGGING_CONFIG["filters"][filter_name] = simple_filter

simple_handler = {
    "class": "logging.StreamHandler",
    "formatter": "airflow",
    "stream": "ext://sys.stdout",
    "filters": [],
}

for handler_name in ("task", "processor", "console", "file.processor"):
    if handler_name in LOGGING_CONFIG.get("handlers", {}):
        LOGGING_CONFIG["handlers"][handler_name] = simple_handler
