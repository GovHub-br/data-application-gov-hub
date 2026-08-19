"""Cliente para a API IBGE Agregados v3.

Cobre todos os endpoints documentados em:
https://servicodados.ibge.gov.br/api/docs/agregados?versao=3

Endpoints suportados:
  GET /agregados                                           → lista pesquisas e agregados
  GET /agregados/{id}/metadados                           → metadados de um agregado
  GET /agregados/{id}/periodos                            → períodos disponíveis
  GET /agregados/{id}/variaveis                           → variáveis disponíveis
  GET /agregados/{id}/localidades/{nivel}                 → localidades por nível geográfico
  GET /agregados/{id}/periodos/{per}/variaveis/{var}     → séries de resultados
"""

import http
import logging
import time
from typing import Any, Optional

from cliente_base import ClienteBase
from safe_request import request_safe

logger = logging.getLogger(__name__)

# Mapeamento dos códigos de nível geográfico para descrições
NIVEIS_GEOGRAFICOS: dict[str, str] = {
    "N1": "Brasil",
    "N2": "Grande Região",
    "N3": "Unidade da Federação",
    "N6": "Município",
    "N7": "Região Metropolitana",
    "N8": "Mesorregião Geográfica",
    "N9": "Microrregião Geográfica",
    "N101": "País",
    "N102": "Região Imediata",
    "N103": "Região Intermediária",
}

_RETRY_SLEEP_S = 1.0


class ClienteIBGE(ClienteBase):
    """Cliente HTTP para a API IBGE Agregados v3."""

    BASE_URL = "https://servicodados.ibge.gov.br/api/v3"

    def __init__(self, sleep_entre_requests: float = _RETRY_SLEEP_S) -> None:
        super().__init__(base_url=self.BASE_URL)
        self.sleep_entre_requests = sleep_entre_requests

    # ------------------------------------------------------------------
    # Endpoint 1: /agregados
    # ------------------------------------------------------------------

    def get_pesquisas(self) -> list[dict[str, Any]]:
        """GET /agregados — retorna todas as pesquisas com seus agregados."""
        status, data = request_safe(self, http.HTTPMethod.GET, "/agregados")
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_pesquisas: status=%s tipo=%s",
                status,
                type(data).__name__,
            )
            return []
        logger.info(
            "[extracao_ibge] get_pesquisas: %s pesquisas retornadas", len(data)
        )
        return data

    def listar_agregados(
        self, pesquisas: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Achata a resposta de get_pesquisas em uma lista plana de agregados.

        Aceita ``pesquisas`` já buscados para evitar uma segunda chamada à API
        quando o chamador já possui os dados.

        Cada item recebe ``pesquisa_id`` e ``pesquisa_nome`` para rastreabilidade.
        """
        if pesquisas is None:
            pesquisas = self.get_pesquisas()
        agregados: list[dict[str, Any]] = []
        for pesquisa in pesquisas:
            pid = pesquisa.get("id", "")
            pnome = pesquisa.get("nome", "")
            for agr in pesquisa.get("agregados", []):
                agregados.append(
                    {
                        "id": agr.get("id"),
                        "nome": agr.get("nome"),
                        "pesquisa_id": pid,
                        "pesquisa_nome": pnome,
                    }
                )
        logger.info(
            "[extracao_ibge] listar_agregados: %s agregados em %s pesquisas",
            len(agregados),
            len(pesquisas),
        )
        return agregados

    # ------------------------------------------------------------------
    # Endpoint 2: /agregados/{id}/metadados
    # ------------------------------------------------------------------

    def get_metadados(self, agregado_id: int | str) -> Optional[dict[str, Any]]:
        """GET /agregados/{id}/metadados."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/metadados"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, dict):
            logger.warning(
                "[extracao_ibge] get_metadados: agregado=%s status=%s",
                agregado_id,
                status,
            )
            return None
        return data

    def get_metadados_batch(
        self, ids: list[int | str]
    ) -> list[dict[str, Any]]:
        """Chama get_metadados para cada ID e achata o resultado.

        Campos aninhados complexos (listas) são convertidos para string JSON
        para compatibilidade com insert_data do ClientPostgresDB.
        """
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            meta = self.get_metadados(agregado_id)
            if meta:
                plano: dict[str, Any] = {"agregado_id": str(agregado_id)}
                for k, v in meta.items():
                    plano[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                resultados.append(plano)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 3: /agregados/{id}/periodos
    # ------------------------------------------------------------------

    def get_periodos(self, agregado_id: int | str) -> list[dict[str, Any]]:
        """GET /agregados/{id}/periodos."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/periodos"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_periodos: agregado=%s status=%s",
                agregado_id,
                status,
            )
            return []
        return data

    def get_periodos_batch(
        self, ids: list[int | str]
    ) -> list[dict[str, Any]]:
        """Busca períodos para cada agregado ID e adiciona ``agregado_id`` em cada registro."""
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            periodos = self.get_periodos(agregado_id)
            for p in periodos:
                row: dict[str, Any] = {"agregado_id": str(agregado_id)}
                for k, v in p.items():
                    row[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                resultados.append(row)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 4: /agregados/{id}/variaveis
    # ------------------------------------------------------------------

    def get_variaveis(self, agregado_id: int | str) -> list[dict[str, Any]]:
        """GET /agregados/{id}/variaveis."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/variaveis"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_variaveis: agregado=%s status=%s",
                agregado_id,
                status,
            )
            return []
        return data

    def get_variaveis_batch(
        self, ids: list[int | str]
    ) -> list[dict[str, Any]]:
        """Busca variáveis para cada agregado ID."""
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            variaveis = self.get_variaveis(agregado_id)
            for v in variaveis:
                row: dict[str, Any] = {"agregado_id": str(agregado_id)}
                for k, val in v.items():
                    row[k] = json.dumps(val, ensure_ascii=False) if isinstance(val, (list, dict)) else val
                resultados.append(row)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 5: /agregados/{id}/localidades/{nivel}
    # ------------------------------------------------------------------

    def get_localidades(
        self, agregado_id: int | str, nivel: str = "N3"
    ) -> list[dict[str, Any]]:
        """GET /agregados/{id}/localidades/{nivel}."""
        status, data = request_safe(
            self, http.HTTPMethod.GET, f"/agregados/{agregado_id}/localidades/{nivel}"
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_localidades: agregado=%s nivel=%s status=%s",
                agregado_id,
                nivel,
                status,
            )
            return []
        return data

    def get_localidades_batch(
        self, ids: list[int | str], nivel: str = "N3"
    ) -> list[dict[str, Any]]:
        """Busca localidades para cada agregado ID no nível geográfico indicado."""
        import json

        resultados: list[dict[str, Any]] = []
        for agregado_id in ids:
            localidades = self.get_localidades(agregado_id, nivel)
            for loc in localidades:
                row: dict[str, Any] = {"agregado_id": str(agregado_id), "nivel": nivel}
                for k, v in loc.items():
                    row[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                resultados.append(row)
            if self.sleep_entre_requests:
                time.sleep(self.sleep_entre_requests)
        return resultados

    # ------------------------------------------------------------------
    # Endpoint 6: /agregados/{id}/periodos/{per}/variaveis/{var}
    # ------------------------------------------------------------------

    def get_resultados(
        self,
        agregado_id: int | str,
        periodos: str = "ultimo",
        variaveis: str = "allxp",
        nivel: str = "N3",
        localidades: str = "all",
    ) -> list[dict[str, Any]]:
        """GET /agregados/{id}/periodos/{per}/variaveis/{var}

        Args:
            agregado_id: ID numérico do agregado IBGE.
            periodos: Período(s) separados por ``|``, ou ``"ultimo"`` para o
                mais recente, ou ``"all"`` para todos.
            variaveis: ID(s) de variável separados por ``|``, ou ``"allxp"``
                para todas.
            nivel: Código de nível geográfico (ex.: ``"N3"`` para UF).
            localidades: Código de localidade dentro do nível, ou ``"all"``.

        Returns:
            Lista de registros de séries de resultados.
        """
        path = f"/agregados/{agregado_id}/periodos/{periodos}/variaveis/{variaveis}"
        params = {"localidades": f"{nivel}[{localidades}]"}
        status, data = request_safe(
            self, http.HTTPMethod.GET, path, params=params
        )
        if status != http.HTTPStatus.OK or not isinstance(data, list):
            logger.warning(
                "[extracao_ibge] get_resultados: agregado=%s periodos=%s status=%s",
                agregado_id,
                periodos,
                status,
            )
            return []
        return data

    def get_resultados_achatar(
        self,
        agregado_id: int | str,
        periodos: str = "ultimo",
        variaveis: str = "allxp",
        nivel: str = "N3",
        localidades: str = "all",
    ) -> list[dict[str, Any]]:
        """Achata a resposta de get_resultados em linhas observacionais.

        A API retorna uma estrutura aninhada:
          variavel → localidade → [{ id, periodo, valor }]

        Este método produz uma linha por combinação (variavel, localidade, período).
        """
        raw = self.get_resultados(
            agregado_id=agregado_id,
            periodos=periodos,
            variaveis=variaveis,
            nivel=nivel,
            localidades=localidades,
        )

        linhas: list[dict[str, Any]] = []
        for variavel in raw:
            var_id = variavel.get("id")
            var_nome = variavel.get("variavel")
            var_unidade = variavel.get("unidade")
            for resultado in variavel.get("resultados", []):
                classificacoes = resultado.get("classificacoes", [])
                for serie in resultado.get("series", []):
                    localidade = serie.get("localidade", {})
                    for periodo, valor in serie.get("serie", {}).items():
                        linhas.append(
                            {
                                "agregado_id": str(agregado_id),
                                "variavel_id": str(var_id),
                                "variavel_nome": var_nome,
                                "unidade": var_unidade,
                                "localidade_id": str(localidade.get("id", "")),
                                "localidade_nome": localidade.get("nome", ""),
                                "nivel": nivel,
                                "periodo": periodo,
                                "valor": valor,
                                "classificacoes": str(classificacoes) if classificacoes else None,
                            }
                        )
        return linhas
