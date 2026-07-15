import logging
import http
from cliente_base import ClienteBase


class ClienteTransfereGov(ClienteBase):
    BASE_URL = "https://api.transferegov.gestao.gov.br/fundoafundo"
    BASE_HEADER = {"accept": "application/json"}
    # PostgREST: "Prefer: count=none" evita o COUNT(*) exato em cada página,
    # reduzindo a carga no banco de origem em endpoints de alto volume
    # (lancamentos/subtransacoes tendem a ter muito mais linhas que planos/relatorios).
    PAGINATION_HEADER = {**BASE_HEADER, "Prefer": "count=none"}
    DEFAULT_PAGE_LIMIT = 1000

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTransfereGov.BASE_URL)
        logging.info(
            "[cliente_transferegov.py] Initialized ClienteTransfereGov with base_url: "
            f"{ClienteTransfereGov.BASE_URL}"
        )

    def get_programa_by_id(self, id_programa: int) -> dict | None:
        """
        Obtem metadados do programa filtrando via PostgREST (eq.)
        """
        endpoint = f"/programa?id_programa=eq.{id_programa}"
        logging.info(f"[cliente_transferegov.py] Fetching programa ID: {id_programa}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and data:
            logging.info(
                f"[cliente_transferegov.py] Successfully fetched programa: {id_programa}"
            )
            # APIs PostgREST sempre retornam listas. Pegamos o primeiro item.
            return data[0] if isinstance(data, list) else data

        logging.warning(
            f"[cliente_transferegov.py] Failed to fetch programa {id_programa}. Status: {status}"
        )
        return None

    def get_planos_acao_by_programa(
        self, id_programa: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """
        Obtem todos os planos de acao vinculados a um programa via FK,
        paginando com os parametros limit/offset ate o fim dos registros.
        """
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        all_planos = []
        offset = 0

        logging.info(
            f"[cliente_transferegov.py] Fetching planos for programa: {id_programa} "
            f"with pagination limit={limit}"
        )

        while True:
            endpoint = (
                f"/plano_acao?id_programa=eq.{id_programa}"
                f"&limit={limit}&offset={offset}"
            )

            status, data = self.request(
                http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
            )

            if status != http.HTTPStatus.OK or not isinstance(data, list):
                logging.warning(
                    f"[cliente_transferegov.py] Failed to fetch planos for programa "
                    f"{id_programa} at offset {offset}. Status: {status}"
                )
                return None

            if not data:
                break

            all_planos.extend(data)
            logging.info(
                f"[cliente_transferegov.py] Retrieved {len(data)} registros "
                f"(offset={offset}). Total acumulado: {len(all_planos)}"
            )

            if len(data) < limit:
                break

            offset += limit

        logging.info(
            f"[cliente_transferegov.py] Successfully fetched {len(all_planos)} "
            f"planos for programa: {id_programa}"
        )
        return all_planos

    def get_relatorios_by_plano_acao(
        self, id_plano_acao: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """
        Obtem todos os relatorios de gestao vinculados a um plano de acao,
        paginando com os parametros limit/offset ate o fim dos registros.
        """
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        all_relatorios = []
        offset = 0

        logging.info(
            f"[cliente_transferegov.py] Fetching relatorios for plano_acao: {id_plano_acao} "
            f"with pagination limit={limit}"
        )

        while True:
            endpoint = (
                f"/relatorio_gestao?id_plano_acao=eq.{id_plano_acao}"
                f"&limit={limit}&offset={offset}"
            )

            status, data = self.request(
                http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
            )

            if status != http.HTTPStatus.OK or not isinstance(data, list):
                logging.warning(
                    f"[cliente_transferegov.py] Failed to fetch relatorios for plano_acao "
                    f"{id_plano_acao} at offset {offset}. Status: {status}"
                )
                return None

            if not data:
                break

            all_relatorios.extend(data)
            logging.info(
                f"[cliente_transferegov.py] Retrieved {len(data)} registros "
                f"(offset={offset}). Total acumulado: {len(all_relatorios)}"
            )

            if len(data) < limit:
                break

            offset += limit

        logging.info(
            f"[cliente_transferegov.py] Successfully fetched {len(all_relatorios)} "
            f"relatorios for plano_acao: {id_plano_acao}"
        )
        return all_relatorios

    def get_lancamentos_financeiros(self, limit: int = DEFAULT_PAGE_LIMIT) -> list | None:
        """
        Obtem todos os lancamentos de gestao financeira (endpoint PostgREST
        /gestao_financeira_lancamentos), paginando com os parametros
        limit/offset ate o fim dos registros.

        NOTA: este endpoint nao possui campo id_plano_acao no payload
        (confirmado na documentacao Swagger oficial) — por isso a busca e
        feita em bloco, sem filtro `eq.` por FK, ao contrario de
        get_planos_acao_by_programa/get_relatorios_by_plano_acao. O
        cruzamento com plano de acao deve ser feito posteriormente via
        cnpj_ente_solicitante_gestao_financeira ou pelo endpoint-ponte
        /plano_acao_dado_bancario.
        """
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        all_lancamentos = []
        offset = 0

        logging.info(
            f"[cliente_transferegov.py] Fetching lancamentos financeiros "
            f"with pagination limit={limit}"
        )

        while True:
            endpoint = f"/gestao_financeira_lancamentos?limit={limit}&offset={offset}"

            status, data = self.request(
                http.HTTPMethod.GET, endpoint, headers=self.PAGINATION_HEADER
            )

            if status != http.HTTPStatus.OK or not isinstance(data, list):
                logging.warning(
                    f"[cliente_transferegov.py] Failed to fetch lancamentos "
                    f"financeiros at offset {offset}. Status: {status}"
                )
                return None

            if not data:
                break

            all_lancamentos.extend(data)
            logging.info(
                f"[cliente_transferegov.py] Retrieved {len(data)} registros "
                f"(offset={offset}). Total acumulado: {len(all_lancamentos)}"
            )

            if len(data) < limit:
                break

            offset += limit

        logging.info(
            f"[cliente_transferegov.py] Successfully fetched {len(all_lancamentos)} "
            f"lancamentos financeiros"
        )
        return all_lancamentos

    def get_subtransacoes_by_lancamento(
        self, id_lancamento_gestao_financeira: int, limit: int = DEFAULT_PAGE_LIMIT
    ) -> list | None:
        """
        Obtem todas as subtransacoes de gestao financeira vinculadas a um
        lancamento (endpoint PostgREST /gestao_financeira_subtransacoes),
        paginando com os parametros limit/offset ate o fim dos registros.
        """
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        all_subtransacoes = []
        offset = 0

        logging.info(
            f"[cliente_transferegov.py] Fetching subtransacoes financeiras for "
            f"lancamento: {id_lancamento_gestao_financeira} with pagination limit={limit}"
        )

        while True:
            endpoint = (
                "/gestao_financeira_subtransacoes"
                f"?id_lancamento_gestao_financeira=eq.{id_lancamento_gestao_financeira}"
                f"&limit={limit}&offset={offset}"
            )

            status, data = self.request(
                http.HTTPMethod.GET, endpoint, headers=self.PAGINATION_HEADER
            )

            if status != http.HTTPStatus.OK or not isinstance(data, list):
                logging.warning(
                    f"[cliente_transferegov.py] Failed to fetch subtransacoes for "
                    f"lancamento {id_lancamento_gestao_financeira} at offset "
                    f"{offset}. Status: {status}"
                )
                return None

            if not data:
                break

            all_subtransacoes.extend(data)
            logging.info(
                f"[cliente_transferegov.py] Retrieved {len(data)} registros "
                f"(offset={offset}). Total acumulado: {len(all_subtransacoes)}"
            )

            if len(data) < limit:
                break

            offset += limit

        logging.info(
            f"[cliente_transferegov.py] Successfully fetched "
            f"{len(all_subtransacoes)} subtransacoes financeiras for lancamento: "
            f"{id_lancamento_gestao_financeira}"
        )
        return all_subtransacoes


class ClienteTransfereGovBackend(ClienteBase):
    BASE_URL = "https://fundos.transferegov.sistema.gov.br/maisbrasil-transferencia-backend/api/public"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTransfereGovBackend.BASE_URL)
        logging.info(
            "[cliente_transferegov_backend.py] Initialized ClienteTransfereGovBackend with base_url: "
            f"{ClienteTransfereGovBackend.BASE_URL}"
        )

    def get_anexos_relatorio(self, id_relatorio_gestao: int) -> list | None:
        """
        Obtem todos os anexos vinculados a um relatorio de gestao.
        """
        endpoint = f"/anexos/relatorio-gestao/{id_relatorio_gestao}"
        logging.info(
            f"[cliente_transferegov_backend.py] Fetching anexos for relatorio: {id_relatorio_gestao}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and data:
            logging.info(
                f"[cliente_transferegov_backend.py] Successfully fetched anexos for relatorio: {id_relatorio_gestao}"
            )
            return data if isinstance(data, list) else [data]

        logging.warning(
            f"[cliente_transferegov_backend.py] Failed to fetch anexos for relatorio {id_relatorio_gestao}. Status: {status}"
        )
        return None
