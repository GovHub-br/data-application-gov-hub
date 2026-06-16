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


def _normalize_chat_ids(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(chat_id) for chat_id in value if chat_id]

    if value:
        return [str(value)]

    return []


def _get_telegram_config() -> tuple[str | None, list[str]]:
    config_raw = Variable.get(TELEGRAM_CONFIG_VARIABLE, default_var=None)

    if config_raw:
        try:
            config = json.loads(config_raw)
            bot_token = config.get("bot_token")
            chat_ids = _normalize_chat_ids(
                config.get("chat_ids", config.get("chat_id"))
            )

            return bot_token, chat_ids
        except json.JSONDecodeError as exc:
            logging.warning(
                "Invalid JSON in Airflow Variable %s: %s",
                TELEGRAM_CONFIG_VARIABLE,
                exc,
            )

    return None, []


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
    bot_token, chat_ids = _get_telegram_config()

    if not bot_token or not chat_ids:
        logging.warning("Telegram notification skipped: missing bot token or chat ids")
        return

    url = TELEGRAM_API_URL.format(token=bot_token)

    for chat_id in chat_ids:
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
            logging.info(
                "Telegram failure notification sent successfully to %s.",
                chat_id,
            )
        except httpx.HTTPError as exc:
            logging.error(
                "Failed to send Telegram failure notification to %s: %s",
                chat_id,
                exc,
            )


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
    reason = context.get("reason")
    state = getattr(task_instance, "state", "unknown")

    if exception:
        exception_text = str(exception)[:MAX_EXCEPTION_CHARS]
    elif reason:
        exception_text = str(reason)[:MAX_EXCEPTION_CHARS]
    elif state == "upstream_failed":
        exception_text = "Cancelada: Falha em task anterior (upstream_failed)"
    else:
        exception_text = "Erro externo/Scheduler (Verifique os logs na interface)"

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
