import http
import logging
from cliente_base import ClienteBase


class ClienteContratos(ClienteBase):
    BASE_URL = "https://contratos.comprasnet.gov.br/api"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteContratos.BASE_URL)
        logging.info(
            "[cliente_contratos.py] Initialized ClienteContratos with base_url: "
            f"{ClienteContratos.BASE_URL}"
        )

    def get_contratos_by_ug(self, ug_code: str) -> list | None:
        """
        Obter todos os contratos ativos de uma UG específica.

        Args:
            ug_code (str): UG code

        Returns:
            list: lista de contratos por ug
        """
        endpoint = f"/contrato/ug/{ug_code}"
        logging.info(f"[cliente_contratos.py] Fetching contratos for UG code: {ug_code}")
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_contratos.py] Successfully fetched contratos for UG code: "
                f"{ug_code}"
            )
            return data
        else:
            logging.warning(
                "[cliente_contratos.py] Failed to fetch contratos for UG code: "
                f"{ug_code} with status: {status}"
            )
            return None

    def get_contratos_inativos_by_ug(self, ug_code: str) -> list | None:
        """
        Obter todos os contratos inativos de uma UG específica.

        Args:
            ug_code (str): UG code

        Returns:
            list: lista de contratos inativos por ug
        """
        endpoint = f"/contrato/inativo/ug/{ug_code}"
        logging.info(f"[cliente_contratos.py] Fetching contratos for UG code: {ug_code}")
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_contratos.py] Successfully fetched contratos for UG code: "
                f"{ug_code}"
            )
            return data
        else:
            logging.warning(
                "[cliente_contratos.py] Failed to fetch contratos for UG code: "
                f"{ug_code} with status: {status}"
            )
            return None

    def get_faturas_by_contrato_id(self, contrato_id: str) -> list | None:
        """
        Obter todas as faturas de um contrato específico.

        Args:
            contrato_id (str): id do contrato

        Returns:
            list: as faturas de um contrato específico.
        """
        endpoint = f"/contrato/{contrato_id}/faturas"
        logging.info(
            f"[cliente_contratos.py] Fetching faturas for contrato ID: {contrato_id}"
        )
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_contratos.py] Successfully fetched faturas for contrato ID: "
                f"{contrato_id}"
            )
            return data
        else:
            logging.warning(
                "[cliente_contratos.py] Failed to fetch faturas for contrato ID: "
                f"{contrato_id} with status: {status}"
            )
            return None

    def get_empenhos_by_contrato_id(self, contrato_id: str) -> list | None:
        """
        Obter todos os empenhos de um contrato específico.

        Args:
            contrato_id (str): id do contrato

        Returns:
            list: os empenhos de um contrato específico.
        """
        endpoint = f"/contrato/{contrato_id}/empenhos"
        logging.info(
            f"[cliente_contratos.py] Fetching empenhos for contrato ID: {contrato_id}"
        )
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_contratos.py] Successfully fetched empenhos for contrato ID: "
                f"{contrato_id}"
            )
            return data
        else:
            logging.warning(
                "[cliente_contratos.py] Failed to fetch empenhos for contrato ID: "
                f"{contrato_id} with status: {status}"
            )
            return None

    def get_cronograma_by_contrato_id(self, contrato_id: str) -> list | None:
        """
        Obter todos os cronogramas de um contrato específico.

        Args:
            contrato_id (str): id do contrato

        Returns:
            list: cronogramas de um contrato específico.
        """
        endpoint = f"/contrato/{contrato_id}/cronograma"
        logging.info(
            f"[cliente_contratos.py] Fetching cronograma for contrato ID: {contrato_id}"
        )
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_contratos.py] Successfully fetched cronograma for contrato ID: "
                f"{contrato_id}"
            )
            return data
        else:
            logging.warning(
                "[cliente_contratos.py] Failed to fetch cronograma for contrato ID: "
                f"{contrato_id} with status: {status}"
            )
            return None

    def get_terceirizados_by_contrato_id(self, contrato_id: str) -> list | None:
        """
        Obter todos os terceirizados de um contrato específico.

        Args:
            contrato_id (str): id do contrato

        Returns:
            list: os terceirizados de um contrato específico.
        """
        endpoint = f"/contrato/{contrato_id}/terceirizados"
        logging.info(
            f"[cliente_contratos.py] Fetching terceirizados for contrato: {contrato_id}"
        )
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_contratos.py] Successfully fetched terceirizados for contrato: "
                f"{contrato_id}"
            )
            return data
        else:
            logging.warning(
                "[cliente_contratos.py] Failed to fetch terceirizados for contrato ID: "
                f"{contrato_id} with status: {status}"
            )
            return None
