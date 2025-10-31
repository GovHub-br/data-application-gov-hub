from typing import Any, Optional, Tuple
from http import HTTPStatus
import logging
import time
import json
import httpx


def request_safe(
    self: Any, method: str, path: str, **kwargs: Any
) -> Tuple[HTTPStatus, Optional[dict | list | str]]:
    """
    Versão tolerante a 204 / corpo vazio / não-JSON / JSON inválido.
    Usa atributos e client do `self` (DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES,
    DEFAULT_SLEEP_SECONDS, client).
    NÃO altera ClienteBase; apenas é chamada passando `self`.
    """
    timeout = kwargs.get("timeout", getattr(self, "DEFAULT_TIMEOUT", 10))
    kwargs["timeout"] = timeout

    max_retries = getattr(self, "DEFAULT_MAX_RETRIES", 3)
    sleep_s = getattr(self, "DEFAULT_SLEEP_SECONDS", 2)

    base_url = getattr(self, "base_url", "")  # opcional, só para logs

    for attempt in range(max_retries):
        try:
            logging.info(
                "[safe_request] Attempt %s %s %s%s | kwargs=%s",
                attempt + 1,
                method,
                base_url,
                path,
                {k: v for k, v in kwargs.items() if k != "timeout"},
            )

            resp = self.client.request(method, path, **kwargs)
            status = HTTPStatus(resp.status_code)

            # 204 ou corpo vazio → não tentar json()
            if status == HTTPStatus.NO_CONTENT or not resp.content:
                logging.info("[safe_request] No content (status=%s)", status)
                return status, None

            # Checar Content-Type
            ct = (resp.headers.get("Content-Type") or "").lower()
            if "application/json" not in ct:
                preview = resp.text[:200] if resp.text else ""
                logging.warning(
                    "[safe_request] Non-JSON content (status=%s, ct=%s) | preview=%r",
                    status,
                    ct,
                    preview,
                )
                return status, resp.text

            # Tentar JSON com fallback
            try:
                return status, resp.json()
            except json.JSONDecodeError as e:
                preview = resp.text[:200] if resp.text else ""
                logging.warning(
                    "[safe_request] Invalid JSON (status=%s): %s | preview=%r",
                    status,
                    e,
                    preview,
                )
                return status, resp.text

        except httpx.HTTPError as e:
            logging.warning("[safe_request] HTTPError on attempt %s: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep((attempt**2) * sleep_s)
            else:
                # última tentativa: não levanta exceção — retorna erro “seguro”
                return HTTPStatus.SERVICE_UNAVAILABLE, f"request_error: {e}"

    # Fallback: não deveria chegar aqui, mas para satisfazer MyPy
    return HTTPStatus.INTERNAL_SERVER_ERROR, "unexpected_error"
