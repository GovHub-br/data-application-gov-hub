import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml


VALID_METADATA_COMMANDS = {"ingest", "profile", "classify"}
DBT_COMMANDS = (
    ("deps",),
    ("build",),
    ("docs", "generate"),
)
REQUIRED_DBT_ARTIFACTS = ("manifest.json", "catalog.json", "run_results.json")


def warm_openmetadata_virtualenv() -> None:
    """Warm the shared virtualenv used by OpenMetadata tasks."""
    logging.basicConfig(level=logging.INFO)

    metadata_bin = Path(sys.executable).with_name("metadata")
    dbt_bin = Path(sys.executable).with_name("dbt")

    logging.info(
        "[openmetadata.execution] Aquecendo virtualenv em %s",
        sys.executable,
    )

    subprocess.run([str(metadata_bin), "--help"], check=True, capture_output=True)
    subprocess.run([str(dbt_bin), "--version"], check=True, capture_output=True)

    logging.info("[openmetadata.execution] Virtualenv aquecido com sucesso.")


def validate_command(metadata_command: str) -> None:
    if metadata_command not in VALID_METADATA_COMMANDS:
        raise ValueError(
            f"Comando inválido: {metadata_command}. Esperado um de {VALID_METADATA_COMMANDS}"
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
    rendered_recipe = rendered_recipe_path.read_text(encoding="utf-8")
    rendered_recipe_data = yaml.safe_load(rendered_recipe)
    source_type = rendered_recipe_data.get("source", {}).get("type", "")
    env = os.environ.copy()

    if source_type == "airflow":
        airflow_logging_config = "openmetadata.airflow_log_config.LOGGING_CONFIG"
        env["AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS"] = airflow_logging_config
        os.environ["AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS"] = airflow_logging_config

    logging.info(
        "[openmetadata.execution] Executando metadata %s -c %s",
        metadata_command,
        rendered_recipe_path,
    )

    if metadata_command == "ingest" and source_type == "airflow":
        execute_metadata_workflow_in_process(rendered_recipe_data)
        return

    subprocess.run(
        [str(metadata_bin), metadata_command, "-c", str(rendered_recipe_path)],
        env=env,
        check=True,
    )


def execute_metadata_workflow_in_process(workflow_config: dict) -> None:
    """Run metadata ingestion directly via Python API.

    This follows the pattern documented by OpenMetadata for
    PythonVirtualenvOperator-based executions.
    """
    from metadata.workflow.metadata import MetadataWorkflow

    logging.info(
        "[openmetadata.execution] Executando workflow em-process via MetadataWorkflow.create(...)"
    )

    workflow = MetadataWorkflow.create(workflow_config)
    try:
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
    finally:
        workflow.stop()


def prepare_dbt_artifacts(project_dir: str, created_tmp_dirs: list[Path]) -> str:
    source_project_dir = Path(project_dir)

    if not source_project_dir.exists():
        raise FileNotFoundError(f"Projeto dbt não encontrado: {source_project_dir}")

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

    for dbt_command in DBT_COMMANDS:
        cmd = [
            "dbt",
            *dbt_command,
            "--project-dir",
            str(project_copy),
            "--profiles-dir",
            str(project_copy),
        ]
        logging.info(
            "[openmetadata.execution] Executando comando: %s",
            " ".join(cmd),
        )
        subprocess.run(
            cmd,
            cwd=str(project_copy),
            env=env,
            check=True,
        )

    target_dir = project_copy / "target"

    for file_name in REQUIRED_DBT_ARTIFACTS:
        artifact = target_dir / file_name
        if not artifact.exists():
            raise FileNotFoundError(f"{file_name} não encontrado em {artifact}")

    return str(target_dir)


def cleanup_tmp_dirs(created_tmp_dirs: list[Path]) -> None:
    for tmp_dir in reversed(created_tmp_dirs):
        try:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
                logging.info(
                    "[openmetadata.execution] Diretório temporário removido: %s",
                    tmp_dir,
                )
        except Exception as exc:
            logging.warning(
                "[openmetadata.execution] Falha ao remover diretório temporário %s: %s",
                tmp_dir,
                exc,
            )


def run_openmetadata_recipe(
    recipe_path: str,
    command: str,
    replacements: dict,
    dbt_project_dir: str = "",
) -> None:
    """Render and execute a single OpenMetadata recipe."""
    logging.basicConfig(level=logging.INFO)

    validate_command(command)

    created_tmp_dirs: list[Path] = []
    workdir = Path(tempfile.mkdtemp(prefix="om_recipe_"))
    created_tmp_dirs.append(workdir)

    try:
        final_replacements = dict(replacements)

        if dbt_project_dir:
            final_replacements["DBT_TARGET_DIR"] = prepare_dbt_artifacts(
                dbt_project_dir,
                created_tmp_dirs,
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
        cleanup_tmp_dirs(created_tmp_dirs)
