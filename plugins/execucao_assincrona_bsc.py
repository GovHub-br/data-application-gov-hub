"""Ponte generica TaskFlow (sincrono) -> ``AsyncBscClient`` (assincrono).

Cada ``@task`` do Airflow roda como uma chamada Python sincrona; para
aproveitar o cliente assincrono (com seu Semaphore/throttle/retry) dentro de
uma task, o padrao e abrir um event loop com ``asyncio.run()`` na fronteira
da task e devolver so o resultado (contagens), nunca os dados brutos, via
XCom. Este modulo concentra esse padrao para nao repeti-lo em cada DAG.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from cliente_bsc import AsyncBscClient, BscRequestError, build_async_client
from file_io_local import save_json_checkpoint

logger = logging.getLogger(__name__)


async def _extrair_lote_async(
    session: aiohttp.ClientSession,
    itens_pendentes: list[Any],
    chamar_api: Callable[[AsyncBscClient, Any], Awaitable[Any]],
    caminho_saida: Callable[[Any], Path],
    tratar_resposta_vazia: Optional[Callable[[BscRequestError], Optional[dict]]],
) -> dict[str, int]:
    client = build_async_client(session)

    async def _extrair_um(item: Any) -> str:
        caminho = caminho_saida(item)
        try:
            dados = await chamar_api(client, item)
            save_json_checkpoint(dados, caminho)
            return "ok"
        except BscRequestError as exc:
            if exc.status_code in (401, 403, 429):
                # Credencial invalida ou rate limit: aborta a execucao
                # inteira, nao adianta seguir tentando os demais itens.
                raise

            if tratar_resposta_vazia is not None:
                marcador = tratar_resposta_vazia(exc)
                if marcador is not None:
                    save_json_checkpoint(marcador, caminho)
                    return "sem_dados"

            logger.error(
                "[execucao_assincrona_bsc] Erro nao retriavel | item=%s | status=%s",
                item,
                exc.status_code,
            )
            return "erro"
        except Exception:
            logger.exception("[execucao_assincrona_bsc] Falha inesperada | item=%s", item)
            return "erro"

    resultados = await asyncio.gather(*[_extrair_um(item) for item in itens_pendentes])

    contagem = {"ok": 0, "sem_dados": 0, "erro": 0}
    for resultado in resultados:
        contagem[resultado] = contagem.get(resultado, 0) + 1
    return contagem


def executar_lote(
    itens_pendentes: list[Any],
    chamar_api: Callable[[AsyncBscClient, Any], Awaitable[Any]],
    caminho_saida: Callable[[Any], Path],
    tratar_resposta_vazia: Optional[Callable[[BscRequestError], Optional[dict]]] = None,
) -> dict[str, int]:
    """Roda ``chamar_api`` para cada item pendente (concorrencia/retry
    controlados pelo ``AsyncBscClient``) e salva cada resposta via
    checkpoint. Retorna a contagem de resultados (ok/sem_dados/erro) --
    nunca os dados brutos, para manter o XCom pequeno."""

    if not itens_pendentes:
        logger.info("[execucao_assincrona_bsc] Nada pendente, nenhuma chamada necessaria")
        return {"ok": 0, "sem_dados": 0, "erro": 0}

    async def _main() -> dict[str, int]:
        async with aiohttp.ClientSession() as session:
            return await _extrair_lote_async(
                session, itens_pendentes, chamar_api, caminho_saida, tratar_resposta_vazia
            )

    contagem = asyncio.run(_main())
    logger.info("[execucao_assincrona_bsc] Resultado do lote: %s", contagem)
    return contagem
