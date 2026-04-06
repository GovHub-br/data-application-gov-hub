from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule

from openmetadata.config import (
    DBT_METADATA_RECIPE,
    GENERIC_RECIPES,
    OPENMETADATA_REQUIREMENTS,
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
        import logging
        import subprocess
        import sys
        from pathlib import Path

        logging.basicConfig(level=logging.INFO)

        metadata_bin = Path(sys.executable).with_name("metadata")
        dbt_bin = Path(sys.executable).with_name("dbt")

        logging.info(
            "[openmetadata_ingestion_dag.py] Aquecendo virtualenv em %s",
            sys.executable,
        )

        subprocess.run([str(metadata_bin), "--help"], check=True, capture_output=True)
        subprocess.run([str(dbt_bin), "--version"], check=True, capture_output=True)

        logging.info(
            "[openmetadata_ingestion_dag.py] Virtualenv aquecido com sucesso."
        )

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
        import logging
        import os
        import shutil
        import subprocess
        import sys
        import tempfile
        from pathlib import Path

        logging.basicConfig(level=logging.INFO)

        created_tmp_dirs = []

        def validate_command(metadata_command: str) -> None:
            valid_commands = {"ingest", "profile", "classify"}
            if metadata_command not in valid_commands:
                raise ValueError(
                    f"Comando inválido: {metadata_command}. Esperado um de {valid_commands}"
                )

        def render_recipe(
            source_recipe_path: str,
            recipe_replacements: dict,
            output_dir: Path,
        ) -> Path:
            recipe_file = Path(source_recipe_path)

            if not recipe_file.exists():
                raise FileNotFoundError(f"Recipe não encontrada: {recipe_file}")

            rendered_recipe = recipe_file.read_text(encoding="utf-8")

            for key, value in recipe_replacements.items():
                if value is None:
                    continue
                rendered_recipe = rendered_recipe.replace(
                    f"${{{key}}}",
                    str(value),
                )

            rendered_recipe_path = output_dir / recipe_file.name
            rendered_recipe_path.write_text(rendered_recipe, encoding="utf-8")

            return rendered_recipe_path

        def execute_metadata(metadata_command: str, rendered_recipe_path: Path) -> None:
            metadata_bin = Path(sys.executable).with_name("metadata")

            logging.info(
                "[openmetadata_ingestion_dag.py] Executando metadata %s -c %s",
                metadata_command,
                rendered_recipe_path,
            )

            subprocess.run(
                [str(metadata_bin), metadata_command, "-c", str(rendered_recipe_path)],
                check=True,
            )

        def prepare_dbt_artifacts(project_dir: str) -> str:
            source_project_dir = Path(project_dir)

            if not source_project_dir.exists():
                raise FileNotFoundError(
                    f"Projeto dbt não encontrado: {source_project_dir}"
                )

            workdir = Path(tempfile.mkdtemp(prefix="om_dbt_"))
            created_tmp_dirs.append(workdir)

            project_copy = workdir / "dbt_project"

            shutil.copytree(
                source_project_dir,
                project_copy,
                ignore=shutil.ignore_patterns(
                    "target",
                    "logs",
                    "dbt_packages",
                    ".venv",
                    "__pycache__",
                ),
            )

            env = os.environ.copy()

            def run_cmd(cmd: list[str]) -> None:
                logging.info(
                    "[openmetadata_ingestion_dag.py] Executando comando: %s",
                    " ".join(cmd),
                )
                subprocess.run(
                    cmd,
                    cwd=str(project_copy),
                    env=env,
                    check=True,
                )

            run_cmd(
                [
                    "dbt",
                    "deps",
                    "--project-dir",
                    str(project_copy),
                    "--profiles-dir",
                    str(project_copy),
                ]
            )

            run_cmd(
                [
                    "dbt",
                    "build",
                    "--project-dir",
                    str(project_copy),
                    "--profiles-dir",
                    str(project_copy),
                ]
            )

            run_cmd(
                [
                    "dbt",
                    "docs",
                    "generate",
                    "--project-dir",
                    str(project_copy),
                    "--profiles-dir",
                    str(project_copy),
                ]
            )

            target_dir = project_copy / "target"

            for file_name in ("manifest.json", "catalog.json", "run_results.json"):
                artifact = target_dir / file_name
                if not artifact.exists():
                    raise FileNotFoundError(
                        f"{file_name} não encontrado em {artifact}"
                    )

            return str(target_dir)

        validate_command(command)

        workdir = Path(tempfile.mkdtemp(prefix="om_recipe_"))
        created_tmp_dirs.append(workdir)

        try:
            final_replacements = dict(replacements)

            if dbt_project_dir:
                final_replacements["DBT_TARGET_DIR"] = prepare_dbt_artifacts(
                    dbt_project_dir
                )

            rendered_recipe_path = render_recipe(
                source_recipe_path=recipe_path,
                recipe_replacements=final_replacements,
                output_dir=workdir,
            )

            execute_metadata(
                metadata_command=command,
                rendered_recipe_path=rendered_recipe_path,
            )
        finally:
            for tmp_dir in reversed(created_tmp_dirs):
                try:
                    if tmp_dir.exists():
                        shutil.rmtree(tmp_dir, ignore_errors=True)
                        logging.info(
                            "[openmetadata_ingestion_dag.py] Diretório temporário removido: %s",
                            tmp_dir,
                        )
                except Exception as exc:
                    logging.warning(
                        "[openmetadata_ingestion_dag.py] Falha ao remover diretório temporário %s: %s",
                        tmp_dir,
                        exc,
                    )

    warmup = warm_openmetadata_virtualenv()

    recipe_tasks = {}

    for recipe in GENERIC_RECIPES + [DBT_METADATA_RECIPE]:
        recipe_tasks[recipe["task_id"]] = run_openmetadata_recipe.override(
            task_id=recipe["task_id"]
        )(
            recipe_path=recipe["recipe_path"],
            command=recipe["command"],
            replacements=recipe["replacements"],
            dbt_project_dir=recipe.get("dbt_project_dir", ""),
        )

    warmup >> recipe_tasks["airflow_metadata"]

    recipe_tasks["airflow_metadata"] >> recipe_tasks["postgres_metadata"]
    recipe_tasks["postgres_metadata"] >> recipe_tasks["dbt_metadata"]
    recipe_tasks["dbt_metadata"] >> recipe_tasks["postgres_profiler"]
    recipe_tasks["postgres_profiler"] >> recipe_tasks["postgres_classifier"]
    recipe_tasks["postgres_classifier"] >> recipe_tasks["superset_metadata"]


openmetadata_ingestion_dag()