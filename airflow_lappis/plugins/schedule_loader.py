from airflow.models import Variable
from datetime import timedelta


def get_dynamic_schedule(dag_id: str, default: str = "@daily") -> str | timedelta:
    """
    Retorna o schedule da Variable 'dynamic_schedules' para a DAG.
    Suporta: 'preset'/'cron' (retorna str) e 'timedelta' (retorna timedelta).
    Se não houver schedule configurado, retorna o valor default (@daily).
    """

    schedules = Variable.get("dynamic_schedules", default_var={}, deserialize_json=True)

    dag_schedule = schedules.get(dag_id)

    if not dag_schedule:
        return default

    dag_type = dag_schedule.get("type")
    dag_value = dag_schedule.get("value")

    if dag_type in ["preset", "cron"]:
        return str(dag_value)

    if dag_type == "timedelta":
        return timedelta(**dag_value)

    raise ValueError(f"Tipo de schedule inválido: {dag_type}")
