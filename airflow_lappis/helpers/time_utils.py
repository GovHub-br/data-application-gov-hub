from datetime import datetime, timezone, timedelta


def brasilia_now_iso() -> str:
    """Retorna o timestamp atual em brasilia no formato ISO8601 (-03:00)."""
    return datetime.now(timezone(timedelta(hours=-3))).isoformat()
