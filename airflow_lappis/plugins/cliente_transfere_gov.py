import http
import logging
from typing import Optional
from cliente_base import ClienteBase


class ClienteTransfereGov(ClienteBase):
    BASE_URL = "https://api.transferegov.gestao.gov.br/transferenciasespeciais/"
    BASE_HEADER = {"accept": "application/json", "User-Agent": "Airflow-GovHub/1.0"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTransfereGov.BASE_URL)
        logging.info(
            "[cliente_transfere_gov.py] Initialized ClienteTransfereGov with base_url: "
            f"{ClienteTransfereGov.BASE_URL}"
        )

    def get_programas_especiais(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter programas especiais com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de programas especiais ou None se falhar
        """
        endpoint = "programa_especial"
        params = {
            "select": "*",
            "order": "id_programa.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching programas especiais with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "programas especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch programas especiais "
                f"with status: {status}"
            )
            return None

    def get_all_programas_especiais(self, page_size: int = 1000) -> list:
        """
        Obter todos os programas especiais com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de programas especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of programas especiais"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} " f"(offset: {offset})"
            )

            data = self.get_programas_especiais(limit=page_size, offset=offset)

            if not data or len(data) == 0:
                logging.info(
                    "[cliente_transfere_gov.py] No more data received. "
                    "Extraction complete."
                )
                break

            all_data.extend(data)
            logging.info(
                f"[cliente_transfere_gov.py] Page {page} fetched: {len(data)} records. "
                f"Total so far: {len(all_data)}"
            )

            # Se recebemos menos registros que o limite, é a última página
            if len(data) < page_size:
                logging.info("[cliente_transfere_gov.py] Last page reached.")
                break

            offset += page_size
            page += 1

        logging.info(
            f"[cliente_transfere_gov.py] Extraction completed. "
            f"Total records: {len(all_data)}"
        )
        return all_data

    def get_planos_acao_especiais_by_programa(
        self, id_programa: int, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter planos de ação especiais por ID do programa com paginação.

        Args:
            id_programa (int): ID do programa
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de planos de ação especiais ou None se falhar
        """
        endpoint = f"plano_acao_especial?id_programa=eq.{id_programa}"
        params = {"select": "*", "limit": limit, "offset": offset}

        logging.info(
            f"[cliente_transfere_gov.py] Fetching planos de ação especiais for "
            f"id_programa={id_programa}, limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                f"planos de ação for programa {id_programa}"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch planos de ação for "
                f"programa {id_programa} with status: {status}"
            )
            return None

    def get_all_planos_acao_especiais_by_programa(
        self, id_programa: int, page_size: int = 1000
    ) -> list:
        """
        Obter todos os planos de ação especiais de um programa com paginação automática.

        Args:
            id_programa (int): ID do programa
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de planos de ação especiais
        """
        all_data = []
        offset = 0

        while True:
            data = self.get_planos_acao_especiais_by_programa(
                id_programa, limit=page_size, offset=offset
            )

            if not data or len(data) == 0:
                break

            all_data.extend(data)

            if len(data) < page_size:
                break

            offset += page_size

        logging.info(
            f"[cliente_transfere_gov.py] Total planos de ação for programa "
            f"{id_programa}: {len(all_data)}"
        )
        return all_data
