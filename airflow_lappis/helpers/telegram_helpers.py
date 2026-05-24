import html
import json
import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from airflow.models import Variable

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_TIMEOUT_SECONDS = 3.0
TELEGRAM_CONFIG_VARIABLE = "telegram_bot_configuracoes"
LOCAL_TIMEZONE = ZoneInfo("America/Sao_Paulo")
MAX_EXCEPTION_CHARS = 500


def _get_telegram_config() -> tuple[str | None, str | None]:
    config_raw = Variable.get(TELEGRAM_CONFIG_VARIABLE, default_var=None)

    if config_raw:
        try:
            config = json.loads(config_raw)
            return config.get("bot_token"), config.get("chat_id")
        except json.JSONDecodeError as exc:
            logging.warning(
                "Invalid JSON in Airflow Variable %s: %s",
                TELEGRAM_CONFIG_VARIABLE,
                exc,
            )

    return None, None


def _format_execution_date(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.strftime("%d/%m/%Y %H:%M:%S")

        return value.astimezone(LOCAL_TIMEZONE).strftime("%d/%m/%Y %H:%M:%S")

    if value:
        return str(value)

    return "unavailable"


def send_telegram_message(message: str) -> None:
    """Envia uma mensagem para o grupo configurado nas Airflow Variables."""
    bot_token, chat_id = _get_telegram_config()

    if not bot_token or not chat_id:
        logging.warning("Telegram notification skipped: missing bot token or chat id")
        return

    url = TELEGRAM_API_URL.format(token=bot_token)
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = httpx.post(
            url,
            json=payload,
            timeout=TELEGRAM_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        logging.info("Telegram failure notification sent successfully.")
    except httpx.HTTPError as exc:
        logging.error("Failed to send Telegram failure notification: %s", exc)


def telegram_failure_callback(context: dict[str, Any]) -> None:
    """Callback do Airflow para notificar falhas de tasks via Telegram."""
    dag = context.get("dag")
    task_instance = context.get("task_instance")

    if dag is None and task_instance is not None:
        task = getattr(task_instance, "task", None)
        dag = getattr(task, "dag", None)

    dag_id = getattr(task_instance, "dag_id", "unknown_dag")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    log_url = getattr(task_instance, "log_url", "")

    tags = getattr(dag, "tags", [])
    tags_text = ", ".join(tags) if tags else "sem tags"

    execution_date = (
        context.get("execution_date")
        or context.get("logical_date")
        or context.get("data_interval_start")
    )
    execution_date_text = _format_execution_date(execution_date)

    exception = context.get("exception")
    exception_text = (
        str(exception)[:MAX_EXCEPTION_CHARS] if exception else "não informado"
    )

    safe_dag_id = html.escape(str(dag_id))
    safe_task_id = html.escape(str(task_id))
    safe_tags = html.escape(tags_text)
    safe_execution_date = html.escape(execution_date_text)
    safe_exception = html.escape(exception_text)
    safe_log_url = html.escape(str(log_url), quote=True)

    message = (
        "<b>Falha em task do Airflow</b>\n\n"
        f"<b>DAG:</b> <code>{safe_dag_id}</code>\n"
        f"<b>Task:</b> <code>{safe_task_id}</code>\n"
        f"<b>Tags:</b> {safe_tags}\n"
        f"<b>Execução:</b> {safe_execution_date}\n"
        f"<b>Erro:</b> <code>{safe_exception}</code>\n"
        f"<b>Logs:</b> {safe_log_url}"
    )

    send_telegram_message(message)
