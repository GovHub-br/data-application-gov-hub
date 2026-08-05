"""Ponte generica TaskFlow (sincrono) -> ``AsyncBscClient`` (assincrono).

Cada ``@task`` do Airflow roda como uma chamada Python sincrona; para
aproveitar o cliente assincrono (com seu Semaphore/throttle/retry) dentro de
uma task, o padrao e abrir um event loop com ``asyncio.run()`` na fronteira
da task e devolver so o resultado (contagens), nunca os dados brutos, via
XCom. Este modulo concentra esse padrao para nao repeti-lo em cada DAG.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, NamedTuple, Optional

import aiohttp

from cliente_bsc import AsyncBscClient, BscRequestError, build_async_client

logger = logging.getLogger(__name__)


class ResultadoItem(NamedTuple):
    """Resultado de uma chamada individual, mantido em memoria (nunca via
    XCom) para quem chama ``executar_lote`` persistir raw + controle no
    Postgres."""

    item: Any
    status: str  # "ok" | "sem_dados" | "erro"
    dados: Optional[dict[str, Any]]
    mensagem_erro: Optional[str]


async def _extrair_lote_async(
    session: aiohttp.ClientSession,
    client: AsyncBscClient,
    itens: list[Any],
    chamar_api: Callable[[AsyncBscClient, Any], Awaitable[Any]],
    tratar_resposta_vazia: Optional[Callable[[BscRequestError], Optional[dict]]],
) -> list[ResultadoItem]:
    async def _extrair_um(item: Any) -> ResultadoItem:
        try:
            dados = await chamar_api(client, item)
            return ResultadoItem(item, "ok", dados, None)
        except BscRequestError as exc:
            if exc.status_code in (401, 403, 429):
                # Credencial invalida ou rate limit: aborta a execucao
                # inteira, nao adianta seguir tentando os demais itens.
                raise

            if tratar_resposta_vazia is not None:
                marcador = tratar_resposta_vazia(exc)
                if marcador is not None:
                    return ResultadoItem(item, "sem_dados", None, marcador.get("erro"))

            logger.error(
                "[execucao_assincrona_bsc] Erro nao retriavel | item=%s | status=%s",
                item,
                exc.status_code,
            )
            return ResultadoItem(item, "erro", None, str(exc))
        except Exception as exc:
            logger.exception("[execucao_assincrona_bsc] Falha inesperada | item=%s", item)
            return ResultadoItem(item, "erro", None, str(exc))

    return await asyncio.gather(*[_extrair_um(item) for item in itens])


def executar_lote(
    itens_pendentes: list[Any],
    chamar_api: Callable[[AsyncBscClient, Any], Awaitable[Any]],
    tratar_resposta_vazia: Optional[Callable[[BscRequestError], Optional[dict]]] = None,
    tamanho_lote: Optional[int] = None,
    ao_concluir_lote: Optional[Callable[[list[ResultadoItem]], None]] = None,
) -> list[ResultadoItem]:
    """Roda ``chamar_api`` para cada item pendente (concorrencia/retry
    controlados pelo ``AsyncBscClient``) e devolve o resultado de cada um.

    Se ``tamanho_lote`` for informado, os itens sao processados em pedacos
    dessa quantidade, chamando ``ao_concluir_lote`` (sincrono) apos cada
    pedaco -- quem chama usa isso para persistir no Postgres
    incrementalmente, em vez de acumular tudo em memoria e gravar so no
    final. Essencial em listas grandes (centenas de milhares de itens,
    horas de execucao): sem isso, qualquer interrupcao no meio perde o
    trabalho inteiro, e nao ha visibilidade de progresso ate o fim.

    Sem ``tamanho_lote``, processa tudo de uma vez (comportamento anterior,
    ainda usado pelas consultas menores de ``extracao_beneficiarios_dag``)."""

    if not itens_pendentes:
        logger.info("[execucao_assincrona_bsc] Nada pendente, nenhuma chamada necessaria")
        return []

    tamanho = tamanho_lote or len(itens_pendentes)
    lotes = [
        itens_pendentes[indice : indice + tamanho]
        for indice in range(0, len(itens_pendentes), tamanho)
    ]

    async def _main() -> list[ResultadoItem]:
        todos_resultados: list[ResultadoItem] = []
        async with aiohttp.ClientSession() as session:
            client = build_async_client(session)
            for numero, lote in enumerate(lotes, start=1):
                resultados_lote = await _extrair_lote_async(
                    session, client, lote, chamar_api, tratar_resposta_vazia
                )
                todos_resultados.extend(resultados_lote)
                logger.info(
                    "[execucao_assincrona_bsc] Lote %d/%d concluido (%d itens)",
                    numero,
                    len(lotes),
                    len(lote),
                )
                if ao_concluir_lote is not None:
                    ao_concluir_lote(resultados_lote)
        return todos_resultados

    resultados = asyncio.run(_main())

    contagem: dict[str, int] = {"ok": 0, "sem_dados": 0, "erro": 0}
    for resultado in resultados:
        contagem[resultado.status] = contagem.get(resultado.status, 0) + 1
    logger.info("[execucao_assincrona_bsc] Resultado do lote: %s", contagem)

    return resultados
