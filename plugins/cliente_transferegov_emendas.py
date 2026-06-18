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

    def get_executores_especiais(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Busca uma fatia global de executores com ordenação para maior performance.
        """
        endpoint = "executor_especial"

        params = {
            "select": "*",
            "order": "id_executor.asc",  # Ordenação por chave primária
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching executores (limit={limit}, offset={offset})"
        )

        status, data = self.request(
            http.HTTPMethod.GET,
            endpoint,
            headers=self.BASE_HEADER,
            params=params,
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Sucesso: {len(data)} registros retornados."
            )
            return data

        logging.warning(
            f"[cliente_transfere_gov.py] Falha na requisição. Status: {status}"
        )
        return None

    def get_all_executores_especiais(self, limit: int = 1000) -> list:
        """
        Extração completa com logs de progresso detalhados.
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Iniciando extração GLOBAL de executores especiais"
        )

        while True:
            data = self.get_executores_especiais(limit=limit, offset=offset)

            if not data or len(data) == 0:
                logging.info("[cliente_transfere_gov.py] Fim dos dados alcançado.")
                break

            all_data.extend(data)

            logging.info(
                f"[cliente_transfere_gov.py] Página {page} processada. "
                f"Total acumulado: {len(all_data)}"
            )

            if len(data) < limit:
                logging.info("[cliente_transfere_gov.py] Última página identificada.")
                break

            offset += limit
            page += 1

        logging.info(
            f"[cliente_transfere_gov.py] Extração finalizada. Total: {len(all_data)}"
        )
        return all_data

    def get_empenhos_especiais_by_plano_acao(
        self, id_plano_acao: int, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter empenhos especiais por ID do plano de ação com paginação.

        Args:
            id_plano_acao (int): ID do plano de ação
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de empenhos especiais ou None se falhar
        """
        endpoint = f"empenho_especial?id_plano_acao=eq.{id_plano_acao}"
        params = {"select": "*", "limit": limit, "offset": offset}

        logging.info(
            f"[cliente_transfere_gov.py] Fetching empenhos especiais for "
            f"id_plano_acao={id_plano_acao}, limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                f"empenhos especiais for plano de ação {id_plano_acao}"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch empenhos especiais for "
                f"plano de ação {id_plano_acao} with status: {status}"
            )
            return None

    def get_empenhos_especiais(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter empenhos especiais com paginação.
        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)
        Returns:
            list: lista de empenhos especiais ou None se falhar
        """
        endpoint = "empenho_especial"
        params = {
            "select": "*",
            "order": "id_empenho.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching empenhos especiais with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "empenhos especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch empenhos especiais "
                f"with status: {status}"
            )
            return None

    def get_all_empenhos_especiais(self, page_size: int = 1000) -> list:
        """
        Obter todos os empenhos especiais com paginação automática.
        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)
        Returns:
            list: lista completa de empenhos especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of empenhos especiais"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} " f"(offset: {offset})"
            )

            data = self.get_empenhos_especiais(limit=page_size, offset=offset)

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

    def get_all_empenhos_especiais_by_plano_acao(
        self, id_plano_acao: int, page_size: int = 1000
    ) -> list:
        """
        Obter todos os empenhos especiais de um plano de ação com paginação automática.

        Args:
            id_plano_acao (int): ID do plano de ação
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de empenhos especiais
        """
        all_data = []
        offset = 0

        while True:
            data = self.get_empenhos_especiais_by_plano_acao(
                id_plano_acao, limit=page_size, offset=offset
            )

            if not data or len(data) == 0:
                break

            all_data.extend(data)

            if len(data) < page_size:
                break

            offset += page_size

        logging.info(
            f"[cliente_transfere_gov.py] Total empenhos especiais for plano de ação "
            f"{id_plano_acao}: {len(all_data)}"
        )
        return all_data

    def get_relatorio_gestao_especial(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """Busca relatórios de gestão de forma global com paginação."""
        endpoint = "relatorio_gestao_especial"
        params = {
            "select": "*",
            "order": "id_relatorio_gestao.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching relatorios with limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} records"
            )
            return data

        logging.warning(f"[cliente_transfere_gov.py] Failed with status: {status}")
        return None

    def get_all_relatorio_gestao_especial(self, page_size: int = 1000) -> list:
        """Obter todos os relatórios com paginação automática global."""
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of relatorio_gestao_especial"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} (offset: {offset})"
            )

            data = self.get_relatorio_gestao_especial(limit=page_size, offset=offset)

            if not data or len(data) == 0:
                logging.info("[cliente_transfere_gov.py] No more data received.")
                break

            all_data.extend(data)
            logging.info(
                f"[cliente_transfere_gov.py] Page {page} fetched. Total so far: {len(all_data)}"
            )

            if len(data) < page_size:
                logging.info("[cliente_transfere_gov.py] Last page reached.")
                break

            offset += page_size
            page += 1

        return all_data

    def get_documentos_habeis_especiais(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter documentos hábeis especiais com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de documentos hábeis especiais ou None se falhar
        """
        endpoint = "documento_habil_especial"
        params = {
            "select": "*",
            "order": "id_dh.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching documentos hábeis especiais with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "documentos hábeis especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch documentos hábeis especiais "
                f"with status: {status}"
            )
            return None

    def get_all_documentos_habeis_especiais(self, page_size: int = 1000) -> list:
        """
        Obter todos os documentos hábeis especiais com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de documentos hábeis especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of "
            "documentos hábeis especiais"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} " f"(offset: {offset})"
            )

            data = self.get_documentos_habeis_especiais(limit=page_size, offset=offset)

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

    def get_documentos_habeis_especiais_by_empenho(
        self, id_empenho: int, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter documentos hábeis especiais por ID do empenho com paginação.

        Args:
            id_empenho (int): ID do empenho
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de documentos hábeis especiais ou None se falhar
        """
        endpoint = f"documento_habil_especial?id_empenho=eq.{id_empenho}"
        params = {"select": "*", "limit": limit, "offset": offset}

        logging.info(
            f"[cliente_transfere_gov.py] Fetching documentos hábeis especiais for "
            f"id_empenho={id_empenho}, limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                f"documentos hábeis especiais for empenho {id_empenho}"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch documentos "
                f"hábeis especiais for empenho {id_empenho} with status: "
                f"{status}"
            )
            return None

    def get_all_documentos_habeis_especiais_by_empenho(
        self, id_empenho: int, page_size: int = 1000
    ) -> list:
        """
        Obter todos os documentos hábeis especiais de um empenho com paginação automática.

        Args:
            id_empenho (int): ID do empenho
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de documentos hábeis especiais
        """
        all_data = []
        offset = 0

        while True:
            data = self.get_documentos_habeis_especiais_by_empenho(
                id_empenho, limit=page_size, offset=offset
            )

            if not data or len(data) == 0:
                break

            all_data.extend(data)

            if len(data) < page_size:
                break

            offset += page_size

        logging.info(
            f"[cliente_transfere_gov.py] Total documentos hábeis especiais for empenho "
            f"{id_empenho}: {len(all_data)}"
        )
        return all_data

    def get_metas_especiais(self, limit: int = 1000, offset: int = 0) -> Optional[list]:
        """
        Obter metas especiais com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de metas especiais ou None se falhar
        """
        endpoint = "meta_especial"
        params = {
            "select": "*",
            "order": "id_meta.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching metas especiais with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "metas especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch metas especiais "
                f"with status: {status}"
            )
            return None

    def get_all_metas_especiais(self, page_size: int = 1000) -> list:
        """
        Obter todas as metas especiais com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de metas especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of metas especiais"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} (offset: {offset})"
            )

            data = self.get_metas_especiais(limit=page_size, offset=offset)

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

            if len(data) < page_size:
                logging.info("[cliente_transfere_gov.py] Last page reached.")
                break

            offset += page_size
            page += 1

        logging.info(
            f"[cliente_transfere_gov.py] Extraction completed. "
            f"Total metas especiais: {len(all_data)}"
        )
        return all_data

    def get_finalidades_especiais(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter finalidades especiais com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de finalidades especiais ou None se falhar
        """
        endpoint = "finalidade_especial"
        params = {
            "select": "*",
            "order": "id_executor.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching finalidades especiais with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "finalidades especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch finalidades especiais "
                f"with status: {status}"
            )
            return None

    def get_all_finalidades_especiais(self, page_size: int = 1000) -> list:
        """
        Obter todas as finalidades especiais com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de finalidades especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of "
            "finalidades especiais"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} " f"(offset: {offset})"
            )

            data = self.get_finalidades_especiais(limit=page_size, offset=offset)

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

    def get_ordens_bancarias_especiais(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter ordens bancárias especiais com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de ordens bancárias especiais ou None se falhar
        """
        endpoint = "ordem_pagamento_ordem_bancaria_especial"
        params = {
            "select": "*",
            "order": "id_op_ob.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching ordens bancárias especiais with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "ordens bancárias especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch ordens bancárias especiais "
                f"with status: {status}"
            )
            return None

    def get_all_ordens_bancarias_especiais(self, page_size: int = 1000) -> list:
        """
        Obter todas as ordens bancárias especiais com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de ordens bancárias especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of "
            "ordens bancárias especiais"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} " f"(offset: {offset})"
            )

            data = self.get_ordens_bancarias_especiais(limit=page_size, offset=offset)

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

    def get_relatorio_gestao_novo_especial(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter relatórios de gestão novo especial com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de relatórios de gestão novo especial ou None se falhar
        """
        endpoint = "relatorio_gestao_novo_especial"
        params = {
            "select": "*",
            "order": "id_relatorio_gestao_novo.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching relatórios de gestão novo with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "relatórios de gestão novo"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch relatórios de gestão novo "
                f"with status: {status}"
            )
            return None

    def get_all_relatorios_gestao_novo_especial(self, page_size: int = 1000) -> list:
        """
        Obter todos os relatórios de gestão novo especial com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de relatórios de gestão novo especial
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of "
            "relatórios de gestão novo"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} " f"(offset: {offset})"
            )

            data = self.get_relatorio_gestao_novo_especial(limit=page_size, offset=offset)

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

    def get_plano_trabalho_especial(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter planos de trabalho especiais com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista de planos de trabalho ou None se falhar
        """
        endpoint = "plano_trabalho_especial"
        params = {
            "select": "*",
            "order": "id_plano_trabalho.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching plano_trabalho_especial with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "planos de trabalho especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch plano_trabalho_especial "
                f"with status: {status}"
            )
            return None

    def get_all_plano_trabalho_especial(self, page_size: int = 1000) -> list:
        """
        Obter todos os planos de trabalho especiais com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa de planos de trabalho especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of "
            "plano_trabalho_especial"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} (offset: {offset})"
            )

            data = self.get_plano_trabalho_especial(limit=page_size, offset=offset)

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

    def get_historico_pagamentos_especiais(
        self, limit: int = 1000, offset: int = 0
    ) -> Optional[list]:
        """
        Obter histórico de pagamentos especiais com paginação.

        Args:
            limit (int): Quantidade de registros por página (padrão: 1000)
            offset (int): Deslocamento inicial (padrão: 0)

        Returns:
            list: lista do histórico de pagamentos especiais ou None se falhar
        """
        endpoint = "historico_pagamento_especial"
        params = {
            "select": "*",
            "order": "id_historico_op_ob.asc",
            "limit": limit,
            "offset": offset,
        }

        logging.info(
            f"[cliente_transfere_gov.py] Fetching histórico de pagamentos especiais with "
            f"limit={limit}, offset={offset}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transfere_gov.py] Successfully fetched {len(data)} "
                "histórico de pagamentos especiais"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transfere_gov.py] Failed to fetch histórico de pagamentos"
                " especiais "
                f"with status: {status}"
            )
            return None

    def get_all_historico_pagamentos_especiais(self, page_size: int = 1000) -> list:
        """
        Obter todo o historico de pagamentos especiais com paginação automática.

        Args:
            page_size (int): Quantidade de registros por requisição (padrão: 1000)

        Returns:
            list: lista completa do histórico de pagamentos especiais
        """
        all_data = []
        offset = 0
        page = 1

        logging.info(
            "[cliente_transfere_gov.py] Starting full extraction of histórico de"
            " pagamentos especiais"
        )

        while True:
            logging.info(
                f"[cliente_transfere_gov.py] Fetching page {page} (offset: {offset})"
            )

            data = self.get_historico_pagamentos_especiais(limit=page_size, offset=offset)

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

            if len(data) < page_size:
                logging.info("[cliente_transfere_gov.py] Last page reached.")
                break

            offset += page_size
            page += 1

        logging.info(
            f"[cliente_transfere_gov.py] Extraction completed. "
            f"Total histórico de pagamentos especiais: {len(all_data)}"
        )
        return all_data
