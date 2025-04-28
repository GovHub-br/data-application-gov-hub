import os
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR

dbt_log_path = "/tmp/dbt_logs"
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/ipea/profiles.yml",
    profile_name="ipea",
    target_name="prod",
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(f"{os.environ['AIRFLOW_REPO_BASE']}/dags/dbt/ipea"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
    ),
    # Expressãp cron para agendar a execução do DAG diariamente às 01:00
    # Futuralmente isso pode ser substituído por um cronograma mais específico
    # com dependências entre os DAGs
    schedule_interval=" 0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="ipea_cosmos_dag",
    default_args={"retries": 2},
)
