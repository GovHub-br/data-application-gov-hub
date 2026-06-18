import http
import logging
from typing import Any
from cliente_base import ClienteBase


class ClienteSenadores(ClienteBase):
    """
    Cliente para consumir a API de Dados Abertos do Senado Federal.
    """

    BASE_URL = "https://legis.senado.leg.br/dadosabertos"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteSenadores.BASE_URL)
        logging.info(
            f"[cliente_senadores.py] Initialized ClienteSenadores em: {ClienteSenadores.BASE_URL}"
        )

    def get_senadores_atuais(self) -> list:
        """
        Obtém a lista de senadores em exercício.
        O Senado geralmente retorna tudo em uma única chamada, sem paginação complexa como a Câmara.
        """
        endpoint = "/senador/lista/atual"
        logging.info("[cliente_senadores.py] Fetching senadores atuais")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and isinstance(data, dict):
            # A estrutura do JSON do Senado é: ListaParlamentarEmExercicio -> Parlamentares -> Parlamentar
            try:
                lista_root = data.get("ListaParlamentarEmExercicio", {})
                parlamentares = lista_root.get("Parlamentares", {}).get("Parlamentar", [])

                if isinstance(parlamentares, dict):
                    parlamentares = [parlamentares]

                logging.info(
                    f"[cliente_senadores.py] Successfully fetched {len(parlamentares)} senadores"
                )
                return parlamentares
            except Exception as e:
                logging.error(
                    f"[cliente_senadores.py] Erro ao parsear JSON do Senado: {e}"
                )
                return []
        else:
            logging.warning(f"[cliente_senadores.py] Failed with status: {status}")
            return []
