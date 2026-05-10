import logging
import time
import httpx
from typing import Any, Optional, Tuple
from http import HTTPStatus


class ClienteBase:
    DEFAULT_TIMEOUT = 10
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_SLEEP_SECONDS = 2

    def __init__(self, base_url: str, headers: Optional[dict] = None) -> None:
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url, headers=headers)
        logging.info(
            f"[cliente_base.py] Initialized ClienteBase with base_url: {base_url}"
        )

    def request(
        self, method: str, path: str, **kwargs: Any
    ) -> Tuple[HTTPStatus, Optional[dict | list]]:
        """
        Faz uma requisição HTTP em até DEFAULT_MAX_RETRIES+1 tentativas.

        Args:
            method (str): HTTP Method.
            path (str): URL path.

        Returns:
            Tuple[HTTPStatus, dict]: status e resposta da requisição HTTP.
        """
        kwargs["timeout"] = kwargs.get("timeout", self.DEFAULT_TIMEOUT)
        response = None

        for attempt in range(self.DEFAULT_MAX_RETRIES):
            try:
                logging.info(
                    f"[cliente_base.py] Attempt {attempt + 1} for {method} "
                    f"{self.base_url}{path} with kwargs: {kwargs}"
                )
                response = self.client.request(method, path, **kwargs)
                response.raise_for_status()
                logging.info(
                    f"[cliente_base.py] Request successful with status "
                    f"{response.status_code}"
                )
                return HTTPStatus(response.status_code), response.json()
            except httpx.HTTPError as e:
                status = response.status_code if response else "Unknown"
                logging.warning(
                    f"[cliente_base.py] API failed with status {status} on "
                    f"attempt {attempt + 1}. Error: {str(e)}"
                )
                if attempt < self.DEFAULT_MAX_RETRIES - 1:
                    time.sleep(attempt**2 * self.DEFAULT_SLEEP_SECONDS)
                else:
                    logging.error(
                        f"[cliente_base.py] API failed after "
                        f"{self.DEFAULT_MAX_RETRIES} attempts. Error: {str(e)}"
                    )
                    raise Exception(
                        "API failed after the maximum number of attempts!"
                    ) from e

        return HTTPStatus.INTERNAL_SERVER_ERROR, None
