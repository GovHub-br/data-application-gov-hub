from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule

from openmetadata.config import (
    ALL_RECIPES,
    OPENMETADATA_REQUIREMENTS,
    RECIPE_PIPELINE,
)


@dag(
    schedule_interval=get_dynamic_schedule("openmetadata_ingestion_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "@arthrok",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["openmetadata", "dbt", "postgres", "superset", "metadata"],
)
def openmetadata_ingestion_dag() -> None:
    """DAG para executar as recipes do OpenMetadata."""

    @task.virtualenv(
        task_id="warm_openmetadata_virtualenv",
        requirements=OPENMETADATA_REQUIREMENTS,
        system_site_packages=False,
        venv_cache_path="/tmp/airflow_venvs",
    )
    def warm_openmetadata_virtualenv() -> None:
        import os
        import sys

        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/dags")

        from openmetadata.execution import warm_openmetadata_virtualenv as execute_warmup

        execute_warmup()

    @task.virtualenv(
        task_id="run_openmetadata_recipe_base",
        requirements=OPENMETADATA_REQUIREMENTS,
        system_site_packages=False,
        venv_cache_path="/tmp/airflow_venvs",
    )
    def run_openmetadata_recipe(
        recipe_path: str,
        command: str,
        replacements: dict,
        dbt_project_dir: str = "",
    ) -> None:
        import os
        import sys
        sys.path.append(f"{os.environ['AIRFLOW_REPO_BASE']}/dags")

        from openmetadata.execution import (
            run_openmetadata_recipe as execute_openmetadata_recipe,
        )

        execute_openmetadata_recipe(
            recipe_path=recipe_path,
            command=command,
            replacements=replacements,
            dbt_project_dir=dbt_project_dir,
        )

    warmup = warm_openmetadata_virtualenv()

    recipe_tasks = {
        recipe.task_id: run_openmetadata_recipe.override(task_id=recipe.task_id)(
            recipe_path=recipe.recipe_path,
            command=recipe.command,
            replacements=dict(recipe.replacements),
            dbt_project_dir=recipe.dbt_project_dir,
        )
        for recipe in ALL_RECIPES
    }

    previous_task = warmup
    for task_id in RECIPE_PIPELINE:
        previous_task >> recipe_tasks[task_id]
        previous_task = recipe_tasks[task_id]


openmetadata_ingestion_dag()
