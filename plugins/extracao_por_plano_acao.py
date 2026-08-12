"""Extracao de endpoints filtrados por ``id_plano_acao``, com propagacao
das chaves de integracao.

As duas tabelas-filhas diretas do plano de acao -- ``plano_acao_meta_minc``
e ``plano_acao_dado_bancario_minc`` -- tem exatamente a mesma forma de
extracao: um GET por plano de acao (a API so filtra por
``id_plano_acao=eq.``), milhares de planos, e a obrigacao da secao 7.1 de
gravar ``id_programa`` e ``cod_ibge`` junto de cada registro mesmo eles
vindo da tabela-pai.

Duas coisas justificam o modulo em vez de repetir o laco nas duas DAGs:

1. **Paralelismo.** Em serie, ~18 mil planos viram ~18 mil GETs sequenciais.
   O mesmo ``ThreadPoolExecutor`` que ``agencias_transferegov`` ja usa
   resolve, e nao ha por que reimplementa-lo duas vezes.
2. **Validacao 9.2.** As chaves vem da tabela-pai, nunca do payload da
   origem -- e quando o payload traz um ``id_programa`` divergente, isso e
   registrado em vez de sobrescrito em silencio.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable

import schemas_minc as schemas
from cliente_postgres import ClientPostgresDB

# Mesmo default de agencias_transferegov.get_contas_agencias_programas: a
# API publica aguenta bem esse nivel de concorrencia.
MAX_WORKERS_PADRAO = 20


def carregar_planos_acao(db: ClientPostgresDB) -> list[dict[str, Any]]:
    """Le de ``plano_acao_minc`` as chaves que precisam ser propagadas.

    E daqui, e nao do payload da origem, que ``id_programa`` e ``cod_ibge``
    saem -- e o que garante a consistencia exigida pela secao 9.2.
    """
    linhas = db.execute_query(
        "SELECT id_plano_acao, id_programa, cod_ibge FROM "
        f"{schemas.SCHEMA_TRANSFEREGOV}.{schemas.TABELA_PLANO_ACAO}"
    )

    return [
        {
            "id_plano_acao": id_plano_acao,
            "id_programa": id_programa,
            "cod_ibge": cod_ibge,
        }
        for id_plano_acao, id_programa, cod_ibge in linhas
    ]


def _propagar_chaves(
    registro: dict[str, Any], plano: dict[str, Any], rotulo: str
) -> dict[str, Any]:
    id_programa_origem = registro.get("id_programa")
    if id_programa_origem is not None and str(id_programa_origem) != str(
        plano["id_programa"]
    ):
        # Validacao 9.2: divergencia entre a origem e a tabela-pai. Vence a
        # tabela-pai, mas o caso fica no log em vez de sumir.
        logging.warning(
            "[extracao_por_plano_acao] %s do plano %s veio com id_programa %s, "
            "diferente do %s registrado em %s",
            rotulo,
            plano["id_plano_acao"],
            id_programa_origem,
            plano["id_programa"],
            schemas.TABELA_PLANO_ACAO,
        )

    registro["id_plano_acao"] = plano["id_plano_acao"]
    registro["id_programa"] = plano["id_programa"]
    registro["cod_ibge"] = plano["cod_ibge"]
    registro["dt_ingest"] = datetime.now().isoformat()
    return registro


def extrair_por_plano_acao(
    planos: list[dict[str, Any]],
    buscar: Callable[[int], list[dict[str, Any]] | None],
    rotulo: str,
    max_workers: int = MAX_WORKERS_PADRAO,
) -> list[dict[str, Any]]:
    """Roda ``buscar(id_plano_acao)`` para cada plano, em paralelo, e devolve
    os registros ja com as chaves de integracao propagadas.

    Um plano sem registros nao e erro (nem todo plano tem meta cadastrada ou
    conta aberta); um plano que falhou na origem e contado e registrado, e a
    carga segue com o que deu certo -- interromper tudo por causa de um plano
    faria a DAG nunca terminar.
    """
    registros: list[dict[str, Any]] = []
    planos_sem_registro = 0
    planos_com_erro = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuro_para_plano = {
            executor.submit(buscar, int(plano["id_plano_acao"])): plano
            for plano in planos
        }

        for futuro in as_completed(futuro_para_plano):
            plano = futuro_para_plano[futuro]

            try:
                encontrados = futuro.result()
            except Exception as exc:
                planos_com_erro += 1
                logging.warning(
                    "[extracao_por_plano_acao] Erro ao buscar %s do plano %s: %s",
                    rotulo,
                    plano["id_plano_acao"],
                    exc,
                )
                continue

            if encontrados is None:
                planos_com_erro += 1
                continue

            if not encontrados:
                planos_sem_registro += 1
                continue

            registros.extend(
                _propagar_chaves(registro, plano, rotulo) for registro in encontrados
            )

    logging.info(
        "[extracao_por_plano_acao] %s: %d registros de %d planos "
        "(%d planos sem registro, %d com erro na origem)",
        rotulo,
        len(registros),
        len(planos),
        planos_sem_registro,
        planos_com_erro,
    )
    return registros
