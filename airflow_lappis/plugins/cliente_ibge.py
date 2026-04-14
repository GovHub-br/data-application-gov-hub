import http
import logging
from typing import Any
from cliente_base import ClienteBase


class ClienteIBGE(ClienteBase):
    """
    Cliente para consumir a API de Dados Abertos do Serviço de Dados IBGE.
    """

    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteIBGE.BASE_URL)
        logging.info(
            "[cliente_ibge.py] Initialized ClienteIBGE with base_url: "
            f"{ClienteIBGE.BASE_URL}"
        )
# Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de referência com rendimento de trabalho, habitualmente recebido no trabalho principal
    def get_rendimento_medio_mensal_real_anual_por_trimestre(self, **params: Any) -> list:
        """
        Obter rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de referência com rendimento de trabalho, habitualmente recebido no trabalho principal do ano inteiro por semestre
        """
        endpoint = "5442/periodos/202501-202504/variaveis/5932?localidades=N1[all]&classificacao=888[47946,47949]"
        logging.info(f"[cliente_ibge.py] Fetching rendimento médio mensal real with params: {params}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_ibge.py] Successfully fetched rendimento médio mensal real"
            )
            return data
        else:
            logging.warning(
                f"[cliente_ibge.py] Failed to fetch rendimento médio mensal real with status: {status}"
            )
            return None
