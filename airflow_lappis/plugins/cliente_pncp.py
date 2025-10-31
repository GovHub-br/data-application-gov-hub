import http
import logging,sys
from typing import Any, Dict, List, Optional, Tuple
from cliente_base import ClienteBase
from typing import Any, Dict, List, Optional
from safe_request import request_safe
import time


# logging.basicConfig(
#     level=logging.INFO,  # ou DEBUG para depurar
#     format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)],
#     force=True  # só no CLI; evita configs antigas bloqueando
# )

logger = logging.getLogger(__name__)


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
        logger.info("[cliente_pncp.py] Initialized ClientePNCP with base_url: %s", ClientePNCP.BASE_URL)


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
        if codigo_modalidade_contratacao is not None:
            params["codigoModalidadeContratacao"] = codigo_modalidade_contratacao
        if uf:
            params["uf"] = uf
        if codigo_municipio_ibge is not None:
            params["codigoMunicipioIbge"] = codigo_municipio_ibge
        if cnpj:
            params["cnpj"] = cnpj
        if codigo_unidade_administrativa is not None:
            params["codigoUnidadeAdministrativa"] = codigo_unidade_administrativa
        if id_usuario is not None:
            params["idUsuario"] = id_usuario

        logger.info(
            "[cliente_pncp.py] Fetching PNCP | params=%s | pagina=%s",
            {k: v for k, v in params.items() if k != "pagina"},
            pagina,
        )

        status, data = request_safe(self,http.HTTPMethod.GET, endpoint, headers={"accept": "application/json"}, params=params)

        # Se não veio 200, não tente decodificar estrutura
        if status != http.HTTPStatus.OK:
            logger.warning(
                "[cliente_pncp.py] HTTP %s | pagina=%s | tipo=%s",
                status, pagina, type(data).__name__
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
            for key in ("data", "items", "results", "content"):
                val = data.get(key)
                if isinstance(val, list):
                    itens = val
                    break

            # tente extrair total de páginas (se existir)
            for k in ("totalPaginas", "total_pages", "totalPages"):
                if isinstance(data.get(k), int):
                    total_paginas = data[k]
                    break

            if not itens:
                logger.warning(
                    "[cliente_pncp.py] 200 mas sem lista reconhecida. keys=%s",
                    list(data.keys())
                )

        # 3) Se vier string/None/outro tipo → trate como vazio
        else:
            logger.warning(
                "[cliente_pncp.py] 200 mas resposta não-JSON-list/dict | tipo=%s",
                type(data).__name__
            )

        logger.info(
            "[cliente_pncp.py] OK | pagina=%s | rows=%s | total_paginas=%s",
            pagina, len(itens), total_paginas
        )
        return itens, total_paginas

