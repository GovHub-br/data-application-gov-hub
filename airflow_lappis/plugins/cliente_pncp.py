import http
import logging
from typing import Any, Dict, List, Optional, Tuple
from cliente_base import ClienteBase
from safe_request import request_safe


# logging.basicConfig(
#     level=logging.INFO,  # ou DEBUG para depurar
#     format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)],
#     force=True  # só no CLI; evita configs antigas bloqueando
# )

logger = logging.getLogger(__name__)


def parse_numero_controle(numero_controle: str) -> Tuple[str, str, str, str]:
    """
    Recebe string no formato 'CNPJ-DIGITO-SEQUENCIAL/ANO'
    e retorna (cnpj, digito, sequencial, ano).
    """
    # Primeiro divide pelo '/' para separar ano
    parte_esquerda, ano = numero_controle.split("/")

    # Depois divide a parte esquerda pelos '-'
    cnpj, digito, sequencial = parte_esquerda.split("-")

    return cnpj, digito, sequencial, ano


def _ymd(ano: int, mes: int, dia: int) -> str:
    """Formata YYYYMMDD com zero-padding correto."""
    return f"{ano:04d}{mes:02d}{dia:02d}"


class ClientePNCP(ClienteBase):
    """
    Cliente para consultar publicações de contratações no PNCP.

    Documentação (resumo do uso):
      - Base: https://pncp.gov.br/api/consulta
      - Endpoint: /v1/contratacoes/publicacao
      - Parâmetros (querystring):
          dataInicial (yyyymmdd)
          dataFinal (yyyymmdd)
          codigoModalidadeContratacao (int)
          uf (sigla do estado, ex.: 'DF')
          codigoMunicipioIbge (int - 7 dígitos)
          cnpj (apenas dígitos)
          codigoUnidadeAdministrativa (int)
          idUsuario (int)
          pagina (int)
    """

    BASE_URL = "https://pncp.gov.br/api"
    BASE_HEADER = {"accept": "*/*"}

    def __init__(self, rate_limit_per_min: int = 120) -> None:
        super().__init__(base_url=ClientePNCP.BASE_URL)
        logger.info(
            "[cliente_pncp.py] Initialized ClientePNCP with base_url: %s",
            ClientePNCP.BASE_URL,
        )

    def get_contratacoes_publicacao(
        self,
        data_inicial: str,
        data_final: str,
        codigo_modalidade_contratacao: Optional[int] = None,
        uf: Optional[str] = None,
        codigo_municipio_ibge: Optional[int] = None,
        cnpj: Optional[str] = None,
        codigo_unidade_administrativa: Optional[int] = None,
        id_usuario: Optional[int] = None,
        pagina: int = 1,
    ) -> Tuple[List[Dict[str, Any]], int]:  # <- mudei o tipo de retorno
        """
        Busca publicações de contratações no PNCP (uma página).

        Returns:
            (lista_itens, total_paginas)
        """
        endpoint = "/consulta/v1/contratacoes/publicacao"

        params: Dict[str, Any] = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "pagina": pagina,
        }
        params["codigoModalidadeContratacao"] = codigo_modalidade_contratacao
        params["cnpj"] = cnpj

        logger.info(
            "[cliente_pncp.py] Fetching PNCP | params=%s | pagina=%s",
            {k: v for k, v in params.items() if k != "pagina"},
            pagina,
        )

        status, data = request_safe(
            self,
            http.HTTPMethod.GET,
            endpoint,
            headers={"accept": "application/json"},
            params=params,
        )

        # Se não veio 200, não tente decodificar estrutura
        if status != http.HTTPStatus.OK:
            logger.warning(
                "[cliente_pncp.py] HTTP %s | pagina=%s | tipo=%s",
                status,
                pagina,
                type(data).__name__,
            )
            return [], 0

        itens: List[Dict[str, Any]] = []
        total_paginas: int = 0

        # 1) Se a API devolver uma lista direta
        if isinstance(data, list):
            itens = data

        # 2) Se vier envelopado em dict
        elif isinstance(data, dict):
            # tente chaves comuns para itens
            key = "data"
            val = data.get(key)
            if isinstance(val, list):
                itens = val

            # tente extrair total de páginas (se existir)
            k = "totalPaginas"
            if isinstance(data.get(k), int):
                total_paginas = data[k]

            if not itens:
                logger.warning(
                    "[cliente_pncp.py] 200 mas sem lista reconhecida. keys=%s",
                    list(data.keys()),
                )

        # 3) Se vier string/None/outro tipo → trate como vazio
        else:
            logger.warning(
                "[cliente_pncp.py] 200 mas resposta não-JSON-list/dict | tipo=%s",
                type(data).__name__,
            )

        logger.info(
            "[cliente_pncp.py] OK | pagina=%s | rows=%s | total_paginas=%s",
            pagina,
            len(itens),
            total_paginas,
        )
        return itens, total_paginas

    def get_contratacoes_publicacao_paginado(
        self,
        data_inicial: str,
        data_final: str,
        codigo_modalidade_contratacao: Optional[int] = None,
        uf: Optional[str] = None,
        codigo_municipio_ibge: Optional[int] = None,
        cnpj: Optional[str] = None,
        codigo_unidade_administrativa: Optional[int] = None,
        id_usuario: Optional[int] = None,
        pagina_inicial: int = 1,
        max_paginas: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Busca publicações de contratações no PNCP, agregando múltiplas páginas.

        Itera páginas até:
          - retornar lista vazia/None,
          - alcançar max_paginas (se fornecido).

        Returns:
            list: Lista agregada com todas as linhas coletadas.
        """
        agregados: List[Dict[str, Any]] = []
        pagina = pagina_inicial
        paginas_coletadas = 0

        while True:
            if max_paginas is not None and paginas_coletadas >= max_paginas:
                logger.info("[cliente_pncp.py] Max de páginas atingido: %s", max_paginas)
                break

            page_data, max_paginas = self.get_contratacoes_publicacao(
                data_inicial=data_inicial,
                data_final=data_final,
                codigo_modalidade_contratacao=codigo_modalidade_contratacao,
                uf=uf,
                codigo_municipio_ibge=codigo_municipio_ibge,
                cnpj=cnpj,
                codigo_unidade_administrativa=codigo_unidade_administrativa,
                id_usuario=id_usuario,
                pagina=pagina,
            )

            if not page_data:
                logger.info(
                    "[cliente_pncp.py] Fim da paginação (vazio/None) na página %s",
                    pagina,
                )
                break

            agregados.extend(page_data)
            paginas_coletadas += 1
            pagina += 1

        logger.info(
            "[cliente_pncp.py] Coleta paginada concluída | total_paginas=%s | "
            "total_registros=%s",
            paginas_coletadas,
            len(agregados),
        )
        return agregados

    def get_contratacoes_publicacao_semestral(
        self,
        data_inicial: str,  # 'YYYYMMDD'
        data_final: str,  # 'YYYYMMDD'
        codigo_modalidade_contratacao: Optional[int] = None,
        uf: Optional[str] = None,
        codigo_municipio_ibge: Optional[int] = None,
        cnpj: Optional[str] = None,
        codigo_unidade_administrativa: Optional[int] = None,
        id_usuario: Optional[int] = None,
        pagina_inicial: int = 1,
        max_paginas: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Varre semestralmente entre data_inicial e data_final usando janelas
        half-open [início, fim).
        Em cada janela, pagina até esgotar os resultados.
        """
        logger.info(
            "[PNCP][semestral] INÍCIO | intervalo_solicitado=[%s, %s] | "
            "filtros: modalidade=%s, uf=%s, ibge=%s, cnpj=%s, ua=%s, usuario=%s | "
            "pagina_inicial=%s, max_paginas=%s",
            data_inicial,
            data_final,
            codigo_modalidade_contratacao,
            uf,
            codigo_municipio_ibge,
            cnpj,
            codigo_unidade_administrativa,
            id_usuario,
            pagina_inicial,
            max_paginas,
        )

        agregados: List[Dict[str, Any]] = []
        try:
            ano_ini = int(data_inicial[:4])
            ano_fim = int(data_final[:4])
        except Exception as e:
            logger.error(
                "[PNCP][semestral] ERRO ao parsear anos de data_inicial/data_final: %s",
                e,
                exc_info=True,
            )
            raise

        limite_inicio = data_inicial
        limite_fim = data_final

        logger.debug(
            "[PNCP][semestral] anos_detectados: ano_ini=%s, ano_fim=%s | "
            "limites_clip=[%s, %s]",
            ano_ini,
            ano_fim,
            limite_inicio,
            limite_fim,
        )

        for ano in range(ano_ini, ano_fim + 1):
            logger.info("[PNCP][semestral] Ano %s → preparando janelas H1 e H2", ano)

            # H1: [ano-01-01, ano-07-01)
            s1_ini = _ymd(ano, 1, 1)
            s1_fim = _ymd(ano, 7, 1)

            # H2: [ano-07-01, (ano+1)-01-01)
            s2_ini = _ymd(ano, 7, 1)
            s2_fim = _ymd(ano + 1, 1, 1)

            # Clip com limites externos
            s1_ini_clip = max(s1_ini, limite_inicio)
            s1_fim_clip = min(s1_fim, limite_fim)
            s2_ini_clip = max(s2_ini, limite_inicio)
            s2_fim_clip = min(s2_fim, limite_fim)

            logger.debug(
                "[PNCP][semestral] Ano %s | H1=[%s, %s) → clip=[%s, %s) | "
                "H2=[%s, %s) → clip=[%s, %s)",
                ano,
                s1_ini,
                s1_fim,
                s1_ini_clip,
                s1_fim_clip,
                s2_ini,
                s2_fim,
                s2_ini_clip,
                s2_fim_clip,
            )

            # --- H1 ---
            if s1_ini_clip < s1_fim_clip:
                logger.info(
                    "[PNCP][semestral] Ano %s | H1 CLIP válido: [%s, %s) → "
                    "iniciando coleta paginada",
                    ano,
                    s1_ini_clip,
                    s1_fim_clip,
                )
                try:
                    page_data = self.get_contratacoes_publicacao_paginado(
                        data_inicial=s1_ini_clip,
                        data_final=s1_fim_clip,
                        codigo_modalidade_contratacao=codigo_modalidade_contratacao,
                        uf=uf,
                        codigo_municipio_ibge=codigo_municipio_ibge,
                        cnpj=cnpj,
                        codigo_unidade_administrativa=codigo_unidade_administrativa,
                        id_usuario=id_usuario,
                        pagina_inicial=pagina_inicial,
                        max_paginas=max_paginas,
                    )
                    n = len(page_data) if page_data else 0
                    logger.info(
                        "[PNCP][semestral] Ano %s | H1 coletado com sucesso | linhas=%s",
                        ano,
                        n,
                    )
                    if page_data:
                        agregados.extend(page_data)
                except Exception as e:
                    logger.error(
                        "[PNCP][semestral] Ano %s | H1 FALHOU: %s", ano, e, exc_info=True
                    )
            else:
                logger.info(
                    "[PNCP][semestral] Ano %s | H1 CLIP vazio/ignorado: [%s, %s)",
                    ano,
                    s1_ini_clip,
                    s1_fim_clip,
                )

            # --- H2 ---
            if s2_ini_clip < s2_fim_clip:
                logger.info(
                    "[PNCP][semestral] Ano %s | H2 CLIP válido: [%s, %s) → "
                    "iniciando coleta paginada",
                    ano,
                    s2_ini_clip,
                    s2_fim_clip,
                )
                try:
                    page_data = self.get_contratacoes_publicacao_paginado(
                        data_inicial=s2_ini_clip,
                        data_final=s2_fim_clip,
                        codigo_modalidade_contratacao=codigo_modalidade_contratacao,
                        uf=uf,
                        codigo_municipio_ibge=codigo_municipio_ibge,
                        cnpj=cnpj,
                        codigo_unidade_administrativa=codigo_unidade_administrativa,
                        id_usuario=id_usuario,
                        pagina_inicial=pagina_inicial,
                        max_paginas=max_paginas,
                    )
                    n = len(page_data) if page_data else 0
                    logger.info(
                        "[PNCP][semestral] Ano %s | H2 coletado com sucesso | linhas=%s",
                        ano,
                        n,
                    )
                    if page_data:
                        agregados.extend(page_data)
                except Exception as e:
                    logger.error(
                        "[PNCP][semestral] Ano %s | H2 FALHOU: %s", ano, e, exc_info=True
                    )
            else:
                logger.info(
                    "[PNCP][semestral] Ano %s | H2 CLIP vazio/ignorado: [%s, %s)",
                    ano,
                    s2_ini_clip,
                    s2_fim_clip,
                )

        logger.info(
            "[PNCP][semestral] FIM | anos=%s..%s | total_linhas=%s",
            ano_ini,
            ano_fim,
            len(agregados),
        )
        return agregados

    def get_itens_e_resultados(
        self, lista_chaves: List[Tuple[str, int, str]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Recebe lista de numeroControlePNCP e retorna:
          - lista com todos os itens de cada contratação
          - lista com todos os resultados dos itens

        Args:
            lista_chaves: lista de tuplas (cnpj, ano, sequencial)

        Returns:
            Tuple:
              - itens (List[Dict])
              - resultados (List[Dict])
        """
        itens_total: List[Dict[str, Any]] = []
        resultados_total: List[Dict[str, Any]] = []

        for numeroControlePNCP in lista_chaves:
            cnpj, digito, sequencial, ano = parse_numero_controle(numeroControlePNCP)
            logger.info(
                "[PNCP][itens/resultados] Iniciando para CNPJ=%s, ano=%s, seq=%s",
                cnpj,
                ano,
                sequencial,
            )

            # 1) Buscar itens da contratação
            endpoint_itens = f"/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
            status, data_itens = request_safe(
                self, http.HTTPMethod.GET, endpoint_itens, headers=self.BASE_HEADER
            )

            if status == http.HTTPStatus.OK and isinstance(data_itens, list):
                for item in data_itens:
                    item["numeroControlePNCP"] = numeroControlePNCP
                itens_total.extend(data_itens)
                logger.info("[PNCP][itens] %s itens coletados", len(data_itens))
            else:
                logger.warning(
                    "[PNCP][itens] Falha ao coletar itens | CNPJ=%s, ano=%s, seq=%s | "
                    "status=%s",
                    cnpj,
                    ano,
                    sequencial,
                    status,
                )
                continue  # pula para próxima chave

            # 2) Consultar quantidade de itens
            endpoint_qtd = (
                f"/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/quantidade"
            )
            status, qtd = request_safe(
                self, http.HTTPMethod.GET, endpoint_qtd, headers=self.BASE_HEADER
            )
            # time.sleep(1)  # Sleep after API call to avoid rate limiting
            if status != http.HTTPStatus.OK or not isinstance(qtd, int):
                logger.warning(
                    "[PNCP][quantidade] Não foi possível obter quantidade de itens | "
                    "CNPJ=%s, ano=%s, seq=%s",
                    cnpj,
                    ano,
                    sequencial,
                )
                continue

            # Após o narrowing acima, qtd é garantidamente int
            qtd_int: int = qtd  # type: ignore[unreachable]

            # 3) Para cada item, buscar resultados
            if qtd_int > 0:
                for numero_item in range(1, qtd_int + 1):
                    endpoint_res = (
                        f"/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/"
                        f"itens/{numero_item}/resultados"
                    )
                    status, data_res = request_safe(
                        self, http.HTTPMethod.GET, endpoint_res, headers=self.BASE_HEADER
                    )
                    # Sleep after API call to avoid rate limiting (commented out)
                    # time.sleep(1)

                    if status == http.HTTPStatus.OK and isinstance(data_res, list):
                        # for r in data_res:
                        #     r["numeroControlePNCP"] = numeroControlePNCP
                        resultados_total.extend(data_res)
                        logger.info(
                            "[PNCP][resultados] Item %s → %s resultados",
                            numero_item,
                            len(data_res),
                        )
                    else:
                        logger.warning(
                            "[PNCP][resultados] Falha no item %s | CNPJ=%s, ano=%s, "
                            "seq=%s",
                            numero_item,
                            cnpj,
                            ano,
                            sequencial,
                        )

        return itens_total, resultados_total