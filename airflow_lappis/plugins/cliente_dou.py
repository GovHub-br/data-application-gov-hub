import json
import logging
import re
from http import HTTPMethod, HTTPStatus
from typing import Optional

import httpx

from cliente_base import ClienteBase

logger = logging.getLogger(__name__)

# ID da tag <script> que contém o JSON embutido na resposta HTML do DOU
_SCRIPT_ID = "_br_com_seatecnologia_in_buscadou_BuscaDouPortlet_params"
_JSON_PATTERN = re.compile(
    rf'<script[^>]+id="{re.escape(_SCRIPT_ID)}"[^>]*>(.*?)</script>',
    re.DOTALL,
)


class ClienteDou(ClienteBase):
    """Cliente HTTP para o portal do Diário Oficial da União (DOU).

    Faz requisições GET ao endpoint de busca do DOU, extrai o bloco JSON
    embutido no HTML retornado e suporta paginação automática.
    """

    BASE_URL = "https://www.in.gov.br"
    ENDPOINT = "/consulta/-/buscar/dou"
    DEFAULT_DELTA = 20  # registros por página

    def __init__(self) -> None:
        super().__init__(base_url=ClienteDou.BASE_URL)

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _formatar_query(self, termos: list[str]) -> str:
        """Envolve cada termo em aspas duplas para forçar busca exata."""
        return " ".join(f'"{termo}"' for termo in termos)

    def _extrair_json_do_html(self, html: str) -> Optional[dict]:
        """Extrai e decodifica o bloco JSON embutido na tag <script> do DOU."""
        match = _JSON_PATTERN.search(html)
        if not match:
            logger.warning(
                "[cliente_dou.py] Tag <script id=%s> não encontrada no HTML.",
                _SCRIPT_ID,
            )
            return None
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError as exc:
            logger.error(
                "[cliente_dou.py] Falha ao decodificar JSON da tag <script>: %s", exc
            )
            return None

    # ------------------------------------------------------------------
    # Método principal de busca
    # ------------------------------------------------------------------

    def buscar_publicacoes(
        self,
        termos: list[str],
        data: str,
        secao: int = 1,
        pagina: int = 1,
        delta: int = DEFAULT_DELTA,
    ) -> list[dict]:
        """Busca publicações do DOU para os termos e data informados.

        Args:
            termos: Lista de termos a buscar. Cada um será envolvido em
                    aspas duplas (busca exata).
            data: Data de publicação no formato ``dd/mm/aaaa``.
            secao: Seção do DOU (1, 2 ou 3). Padrão: 1.
            pagina: Número da página (começa em 1).
            delta: Quantidade de registros por página.

        Returns:
            Lista de dicts com os itens retornados pelo DOU.
            Retorna lista vazia se não houver resultados ou em caso de erro.
        """
        query = self._formatar_query(termos)
        params = {
            "q": query,
            "exactDate": data,
            "secao": f"secao{secao}",
            "currentPage": pagina,
            "delta": delta,
        }

        logger.info(
            "[cliente_dou.py] Buscando DOU | data=%s | secao=%s | pagina=%s | "
            "termos=%s",
            data,
            secao,
            pagina,
            termos,
        )

        # O endpoint retorna HTML, portanto não podemos usar o método padrão
        # `request()` da ClienteBase (que chama `.json()` no final).
        # Fazemos a requisição diretamente via httpx, com o mecanismo de retry
        # do ClienteBase inativo para essa chamada específica.
        try:
            response = self.client.request(
                HTTPMethod.GET,
                self.ENDPOINT,
                params=params,
                timeout=self.DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error(
                "[cliente_dou.py] Erro HTTP ao consultar o DOU: %s", exc
            )
            raise

        # O DOU retorna HTML com o JSON embutido numa tag <script>
        dados_json = self._extrair_json_do_html(response.text)
        if dados_json is None:
            return []

        itens = dados_json.get("jsonArray", [])
        if not isinstance(itens, list):
            logger.warning(
                "[cliente_dou.py] Campo 'jsonArray' não é uma lista: %s",
                type(itens),
            )
            return []

        logger.info(
            "[cliente_dou.py] %d publicações encontradas (página %s).",
            len(itens),
            pagina,
        )
        return itens

    def buscar_todas_publicacoes(
        self,
        termos: list[str],
        data: str,
        secao: int = 1,
        delta: int = DEFAULT_DELTA,
    ) -> list[dict]:
        """Itera por todas as páginas e retorna a lista completa de publicações.

        Args:
            termos: Lista de termos a buscar (busca exata com aspas duplas).
            data: Data de publicação no formato ``dd/mm/aaaa``.
            secao: Seção do DOU (1, 2 ou 3). Padrão: 1.
            delta: Registros por página.

        Returns:
            Lista consolidada com todos os registros de todas as páginas.
        """
        todas: list[dict] = []
        pagina = 1

        while True:
            itens = self.buscar_publicacoes(
                termos=termos,
                data=data,
                secao=secao,
                pagina=pagina,
                delta=delta,
            )
            if not itens:
                logger.info(
                    "[cliente_dou.py] Paginação encerrada na página %s "
                    "(sem mais resultados).",
                    pagina,
                )
                break

            todas.extend(itens)

            if len(itens) < delta:
                logger.info(
                    "[cliente_dou.py] Última página alcançada (página %s).",
                    pagina,
                )
                break

            pagina += 1

        logger.info(
            "[cliente_dou.py] Total de publicações coletadas: %d.", len(todas)
        )
        return todas
