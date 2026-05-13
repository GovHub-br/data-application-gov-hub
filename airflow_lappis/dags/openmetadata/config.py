import os
from dataclasses import dataclass
from typing import Mapping

AIRFLOW_REPO_BASE = os.environ["AIRFLOW_REPO_BASE"]

OPENMETADATA_RECIPES_DIR = f"{AIRFLOW_REPO_BASE}/dags/openmetadata/recipes"
DBT_IPEA_DIR = f"{AIRFLOW_REPO_BASE}/dags/dbt/ipea"

OPENMETADATA_REQUIREMENTS = [
    "openmetadata-ingestion[dbt,postgres,superset,airflow,pii-processor]==1.12.1",
    "asyncpg",
    "PyYAML>=6.0",
    "cachetools",
    "presidio_analyzer",
    "psycopg2-binary",
    "google-cloud-bigquery",
    "keyring==25.6.0",
    "jaraco.context==6.0.1",
    "jaraco.functools==4.1.0",
    "jaraco.classes==3.4.0",
]


@dataclass(frozen=True)
class RecipeDefinition:
    task_id: str
    recipe_path: str
    command: str
    replacements: Mapping[str, str]
    dbt_project_dir: str = ""


COMMON_REPLACEMENTS = {
    "OM_HOST": "{{ var.value.OM_HOST }}",
    "DB_DW_HOST": os.environ["DB_DW_HOST"],
    "DB_DW_PORT": os.environ["DB_DW_PORT"],
    "DB_DW_USER": os.environ["DB_DW_USER"],
    "DB_DW_PASSWORD": os.environ["DB_DW_PASSWORD"],
    "DB_DW_DBNAME": os.environ["DB_DW_DBNAME"],
    "AIRFLOW_HOST_PORT": os.environ.get("AIRFLOW_HOST_PORT", "http://localhost:8080"),
    "AIRFLOW_DB_HOST_PORT": os.environ.get("AIRFLOW_DB_HOST_PORT", "postgres:5432"),
    "AIRFLOW_DB_USERNAME": os.environ.get("POSTGRES_USER", "airflow"),
    "AIRFLOW_DB_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    "AIRFLOW_DB_DATABASE": os.environ.get("AIRFLOW_DB_DATABASE", "airflow"),
    "SUPERSET_HOST_PORT": "{{ var.value.SUPERSET_HOST_PORT }}",
    "SUPERSET_USERNAME": "{{ var.value.SUPERSET_USERNAME }}",
    "SUPERSET_PASSWORD": "{{ var.value.SUPERSET_PASSWORD }}",
}

INGESTION_REPLACEMENTS = {
    **COMMON_REPLACEMENTS,
    "INGESTION_TOKEN": "{{ var.value.INGESTION_TOKEN }}",
}

AIRFLOW_METADATA_RECIPE = RecipeDefinition(
    task_id="airflow_metadata",
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/airflow_metadata.yaml",
    command="ingest",
    replacements=INGESTION_REPLACEMENTS,
)

POSTGRES_METADATA_RECIPE = RecipeDefinition(
    task_id="postgres_metadata",
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_metadata.yaml",
    command="ingest",
    replacements=INGESTION_REPLACEMENTS,
)

POSTGRES_PROFILER_RECIPE = RecipeDefinition(
    task_id="postgres_profiler",
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_profiler.yaml",
    command="profile",
    replacements={
        **COMMON_REPLACEMENTS,
        "PROFILER_TOKEN": "{{ var.value.PROFILER_TOKEN }}",
    },
)

POSTGRES_CLASSIFIER_RECIPE = RecipeDefinition(
    task_id="postgres_classifier",
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_classifier.yaml",
    command="classify",
    replacements={
        **COMMON_REPLACEMENTS,
        "CLASSIFICATION_TOKEN": "{{ var.value.CLASSIFICATION_TOKEN }}",
    },
)

SUPERSET_METADATA_RECIPE = RecipeDefinition(
    task_id="superset_metadata",
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/superset_metadata.yaml",
    command="ingest",
    replacements=INGESTION_REPLACEMENTS,
)

DBT_METADATA_RECIPE = RecipeDefinition(
    task_id="dbt_metadata",
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/dbt_metadata.yaml",
    command="ingest",
    replacements=INGESTION_REPLACEMENTS,
    dbt_project_dir=DBT_IPEA_DIR,
)

GENERIC_RECIPES = (
    AIRFLOW_METADATA_RECIPE,
    POSTGRES_METADATA_RECIPE,
    POSTGRES_PROFILER_RECIPE,
    POSTGRES_CLASSIFIER_RECIPE,
    SUPERSET_METADATA_RECIPE,
)

ALL_RECIPES = GENERIC_RECIPES + (DBT_METADATA_RECIPE,)

RECIPE_PIPELINE = (
    "airflow_metadata",
    "postgres_metadata",
    "dbt_metadata",
    "postgres_profiler",
    "postgres_classifier",
    "superset_metadata",
)
