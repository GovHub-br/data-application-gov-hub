import http
import logging
from typing import Any
from cliente_base import ClienteBase


class ClientePartidos(ClienteBase):
    """
    Cliente para consumir a API de Dados Abertos da Camara dos Deputados para
    pegar a logo dos partidos.
    """

    BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClientePartidos.BASE_URL)
        logging.info(
            "[cliente_partidos.py] Initialized ClientePartidos with base_url: "
            f"{ClientePartidos.BASE_URL}"
        )

    def get_partidos(self, **params: Any) -> list:
        """
        Obter lista de partidos
        """
        endpoint = "/partidos"
        logging.info(f"[cliente_partidos.py] Fetching partidos with params: {params}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, dict):
            partidos: list[dict[str, Any]] = data.get("dados", [])
            logging.info(
                f"[cliente_partidos.py] Successfully fetched {len(partidos)} partidos"
            )
            return partidos
        else:
            logging.warning(
                f"[cliente_partidos.py] Failed to fetch partidos with status: {status}"
            )
            return None

    def get_all_partidos(self) -> list:
        """
        Itera por todas as páginas da API e retorna a lista completa de partidos.
        """
        all_partidos = []
        pagina = 1

        while True:
            params = {
                "pagina": pagina,
                "itens": 100,
                "ordem": "ASC",
                "ordenarPor": "sigla",
            }
            partidos = self.get_partidos(**params)

            if not partidos:
                break

            all_partidos.extend(partidos)

            if len(partidos) < 100:
                break

            pagina += 1

        return all_partidos

    def get_partido_by_id(self, partido_id: int) -> dict:
        """
        Obter detalhes de um partido específico pelo ID
        """
        endpoint = f"/partidos/{partido_id}"
        logging.info(f"[cliente_partidos.py] Fetching partido ID: {partido_id}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and isinstance(data, dict):
            partido: dict[str, Any] = data.get("dados", {})
            return partido
        else:
            logging.warning(
                f"[cliente_partidos.py] Failed to fetch partido {partido_id} "
                f"with status: {status}"
            )
            return None
