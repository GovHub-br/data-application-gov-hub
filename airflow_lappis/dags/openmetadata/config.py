import os

AIRFLOW_REPO_BASE = os.environ["AIRFLOW_REPO_BASE"]

OPENMETADATA_RECIPES_DIR = f"{AIRFLOW_REPO_BASE}/dags/openmetadata/recipes"
DBT_IPEA_DIR = f"{AIRFLOW_REPO_BASE}/dags/dbt/ipea"

OPENMETADATA_REQUIREMENTS = [
    "openmetadata-ingestion[dbt,postgres,superset,airflow,pii-processor]==1.12.1",
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

COMMON_REPLACEMENTS = {
    "OM_HOST": "{{ var.value.OM_HOST }}",
    "DB_DW_HOST": os.environ["DB_DW_HOST"],
    "DB_DW_PORT": os.environ["DB_DW_PORT"],
    "DB_DW_USER": os.environ["DB_DW_USER"],
    "DB_DW_PASSWORD": os.environ["DB_DW_PASSWORD"],
    "DB_DW_DBNAME": os.environ["DB_DW_DBNAME"],
    "SUPERSET_HOST_PORT": "{{ var.value.SUPERSET_HOST_PORT }}",
    "SUPERSET_USERNAME": "{{ var.value.SUPERSET_USERNAME }}",
    "SUPERSET_PASSWORD": "{{ var.value.SUPERSET_PASSWORD }}",
}

AIRFLOW_METADATA_RECIPE = {
    "task_id": "airflow_metadata",
    "recipe_path": f"{OPENMETADATA_RECIPES_DIR}/airflow_metadata.yaml",
    "command": "ingest",
    "replacements": {
        **COMMON_REPLACEMENTS,
        "INGESTION_TOKEN": "{{ var.value.INGESTION_TOKEN }}",
    },
}

POSTGRES_METADATA_RECIPE = {
    "task_id": "postgres_metadata",
    "recipe_path": f"{OPENMETADATA_RECIPES_DIR}/postgres_metadata.yaml",
    "command": "ingest",
    "replacements": {
        **COMMON_REPLACEMENTS,
        "INGESTION_TOKEN": "{{ var.value.INGESTION_TOKEN }}",
    },
}

POSTGRES_PROFILER_RECIPE = {
    "task_id": "postgres_profiler",
    "recipe_path": f"{OPENMETADATA_RECIPES_DIR}/postgres_profiler.yaml",
    "command": "profile",
    "replacements": {
        **COMMON_REPLACEMENTS,
        "PROFILER_TOKEN": "{{ var.value.PROFILER_TOKEN }}",
    },
}

POSTGRES_CLASSIFIER_RECIPE = {
    "task_id": "postgres_classifier",
    "recipe_path": f"{OPENMETADATA_RECIPES_DIR}/postgres_classifier.yaml",
    "command": "classify",
    "replacements": {
        **COMMON_REPLACEMENTS,
        "CLASSIFICATION_TOKEN": "{{ var.value.CLASSIFICATION_TOKEN }}",
    },
}

SUPERSET_METADATA_RECIPE = {
    "task_id": "superset_metadata",
    "recipe_path": f"{OPENMETADATA_RECIPES_DIR}/superset_metadata.yaml",
    "command": "ingest",
    "replacements": {
        **COMMON_REPLACEMENTS,
        "INGESTION_TOKEN": "{{ var.value.INGESTION_TOKEN }}",
    },
}

DBT_METADATA_RECIPE = {
    "task_id": "dbt_metadata",
    "recipe_path": f"{OPENMETADATA_RECIPES_DIR}/dbt_metadata.yaml",
    "command": "ingest",
    "replacements": {
        **COMMON_REPLACEMENTS,
        "INGESTION_TOKEN": "{{ var.value.INGESTION_TOKEN }}",
    },
    "dbt_project_dir": DBT_IPEA_DIR,
}

GENERIC_RECIPES = [
    AIRFLOW_METADATA_RECIPE,
    POSTGRES_METADATA_RECIPE,
    POSTGRES_PROFILER_RECIPE,
    POSTGRES_CLASSIFIER_RECIPE,
    SUPERSET_METADATA_RECIPE,
]
