import http
import logging
from cliente_base import ClienteBase


class ClienteTed(ClienteBase):
    BASE_URL = "https://api.transferegov.gestao.gov.br/ted/"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTed.BASE_URL)

    def get_ted_by_programa_beneficiario(self, tx_codigo_siorg: str) -> list | None:

        endpoint = f"programa_beneficiario?tx_codigo_siorg=eq.{tx_codigo_siorg}"
        logging.info(
            f"[cliente_ted.py] Fetching ted for programa beneficiario: {tx_codigo_siorg}"
        )
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_ted.py] Successfully fetched ted for programa beneficiario: "
                f"{tx_codigo_siorg}"
            )
            return data
        else:
            logging.warning(
                "[cliente_ted.py] Failed to fetch ted for programa beneficiario: "
                f"{tx_codigo_siorg} with status: {status}"
            )
            return None

    def get_programa_by_id_programa(self, id_programa: str) -> list | None:

        endpoint = f"programa?id_programa=eq.{id_programa}"
        logging.info(f"[cliente_ted.py] Fetching programa for id_programa: {id_programa}")
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_ted.py] Successfully fetched programa for id_programa: "
                f"{id_programa}"
            )
            return data
        else:
            logging.warning(
                "[cliente_ted.py] Failed to fetch programa for id_programa: "
                f"{id_programa} with status: {status}"
            )
            return None

    def get_notas_de_credito_by_ug(self, ug_code: int) -> list | None:
        endpoint_1 = f"nota_credito?cd_ug_favorecida_nota=eq.{ug_code}"
        endpoint_2 = f"nota_credito?cd_ug_emitente_nota=eq.{ug_code}"

        logging.info(f"Buscando notas de crédito para UG: {ug_code}")

        status_1, data_1 = self.request(
            http.HTTPMethod.GET, endpoint_1, headers=self.BASE_HEADER
        )
        status_2, data_2 = self.request(
            http.HTTPMethod.GET, endpoint_2, headers=self.BASE_HEADER
        )

        if status_1 == http.HTTPStatus.OK and isinstance(data_1, list):
            logging.info(f"Notas de crédito (favorecida) obtidas para UG {ug_code}")
        else:
            logging.warning(f"Falha ao buscar notas de crédito - Status: {status_1}")
            data_1 = []

        if status_2 == http.HTTPStatus.OK and isinstance(data_2, list):
            logging.info(f"Notas de crédito (emitente) obtidas para UG {ug_code}")
        else:
            logging.warning(f"Falha ao buscar notas de crédito - Status: {status_2}")
            data_2 = []

        data = data_1 + data_2
        return data if data else None

    def get_programacao_financeira_by_ug(self, ug_code: int) -> list | None:
        endpoint_1 = f"programacao_financeira?ug_favorecida_programacao=eq.{ug_code}"
        endpoint_2 = f"programacao_financeira?ug_emitente_programacao=eq.{ug_code}"

        logging.info(f"Buscando programação financeira para UG: {ug_code}")

        status_1, data_1 = self.request(
            http.HTTPMethod.GET, endpoint_1, headers=self.BASE_HEADER
        )
        status_2, data_2 = self.request(
            http.HTTPMethod.GET, endpoint_2, headers=self.BASE_HEADER
        )

        if status_1 == http.HTTPStatus.OK and isinstance(data_1, list):
            logging.info(f"Programação financeira (favorecida) obtida para UG {ug_code}")
        else:
            logging.warning(
                f"Falha ao buscar programação financeira - Status: {status_1}"
            )
            data_1 = []

        if status_2 == http.HTTPStatus.OK and isinstance(data_2, list):
            logging.info(f"Programação financeira (emitente) obtida para UG {ug_code}")
        else:
            logging.warning(
                f"Falha ao buscar programação financeira - Status: {status_2}"
            )
            data_2 = []

        data = data_1 + data_2
        return data if data else None

    def get_planos_acao_by_id_programa(self, id_programa: str) -> list | None:

        endpoint = f"plano_acao?id_programa=eq.{id_programa}"
        logging.info(
            f"[cliente_ted.py] Fetching planos de ação for id_programa: {id_programa}"
        )
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                "[cliente_ted.py] Successfully fetched planos de ação for id_programa: "
                f"{id_programa}"
            )
            return data
        else:
            logging.warning(
                "[cliente_ted.py] Failed to fetch planos de ação for id_programa: "
                f"{id_programa} with status: {status}"
            )
            return None

    def get_programas_by_sigla_unidade_descentralizadora(self, sigla: str) -> list | None:
        endpoint = f"programa?sigla_unidade_descentralizadora=eq.{sigla}"
        logging.info(f"Fetching programas for sigla_unidade_descentralizadora: {sigla}")
        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )
        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"Successfully fetched programas for sigla_unidade_descentralizadora: "
                f"{sigla}"
            )
            return data
        else:
            logging.warning(
                f"Failed to fetch programas for sigla_unidade_descentralizadora: "
                f"{sigla} with status: {status}"
            )
            return None
