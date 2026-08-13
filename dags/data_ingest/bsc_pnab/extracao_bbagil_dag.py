import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

import schemas_minc as schemas
from agencias_bbagil import gerar_periodos_mensais
from cliente_bsc import AsyncBscClient, BscRequestError, is_empty_extrato_response
from cliente_postgres import ClientPostgresDB
from execucao_assincrona_bsc import ResultadoItem, executar_lote
from file_io_local import flatten_records
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

# Persiste no Postgres a cada N itens processados, em vez de acumular tudo
# em memoria e gravar so no final -- em listas de centenas de milhares de
# itens (horas de execucao), sem isso qualquer interrupcao no meio perde o
# trabalho inteiro e nao ha visibilidade de progresso ate o fim.
TAMANHO_LOTE_PERSISTENCIA = 2000

default_args = {
    "owner": "Caio Borges",
    # Alto de proposito: o BSC aplica um bloqueio temporario apos uso
    # sustentado (~20-40min de chamadas continuas, independente do
    # throttle), que leva alguns minutos pra esfriar. Com checkpoint em
    # lote (TAMANHO_LOTE_PERSISTENCIA) cada retry e barato -- so refaz o
    # que nao foi persistido desde o ultimo lote -- entao e seguro deixar
    # tentar de novo por muitas horas sem supervisao, em vez de desistir
    # depois de poucas tentativas.
    "retries": 100,
    "retry_delay": timedelta(minutes=10),
}


def _marcar_extrato_sem_dados(exc: BscRequestError) -> dict[str, Any] | None:
    if is_empty_extrato_response(exc):
        return {"status": "sem_dados", "erro": exc.response_text}
    return None


async def _chamar_extrato(client: AsyncBscClient, item: Any) -> Any:
    # A resposta do BSC traz agencia/conta/saldo/transactions, mas nao
    # identifica o ente/plano de acao/periodo de origem -- injeta aqui para
    # que a linha de cada transacao na tabela raw carregue esses campos
    # (usados no checkpoint e nos filtros/agrupamento do fato).
    ente, (periodo_inicial, periodo_final) = item
    resposta = await client.bbagil_extrato_orgao_controle(
        agencia=int(ente["agencia"]),
        numero_conta=int(ente["conta"]),
        periodo_inicial=periodo_inicial,
        periodo_final=periodo_final,
    )
    resposta["id_plano_acao"] = ente["id_plano_acao"]
    resposta["id_programa"] = ente.get("id_programa")
    resposta["codigo_programa"] = ente.get("codigo_programa")
    resposta["nome_programa"] = ente.get("nome_programa")
    # Chaves de integracao exigidas pela secao 7.2 para o extrato.
    resposta["id_plano_acao_dado_bancario"] = ente.get("id_plano_acao_dado_bancario")
    resposta["id_agencia_conta"] = ente.get("id_agencia_conta")
    resposta["cod_ibge"] = ente.get("cod_ibge")
    resposta["periodo_inicial"] = periodo_inicial
    resposta["periodo_final"] = periodo_final
    return resposta


async def _chamar_subtransacao(client: AsyncBscClient, item: dict[str, Any]) -> Any:
    resposta = await client.bbagil_extrato_sub_lancamentos_orgao_controle(
        agencia=str(item["agencia"]),
        numero_conta=str(item["conta"]),
        id_transaction=str(item["id"]),
    )
    # Mesma injecao do extrato: a resposta nao identifica o plano de acao/
    # transacao-mae de origem, necessarios para o checkpoint e para a chave
    # da tabela raw (o "id" da subtransacao e sequencial por transacao-mae,
    # nao global). As demais chaves sao as exigidas pela secao 7.2.
    resposta["id_plano_acao"] = item["id_plano_acao"]
    resposta["id_transacao_pai"] = item["id"]
    resposta["id_programa"] = item.get("id_programa")
    resposta["id_plano_acao_dado_bancario"] = item.get("id_plano_acao_dado_bancario")
    resposta["id_agencia_conta"] = item.get("id_agencia_conta")
    resposta["cod_ibge"] = item.get("cod_ibge")
    return resposta


# Uma conta por plano de acao, priorizando a ativa. A tabela guarda TODAS as
# contas de cada plano (secao 7.1), mas o extrato e consultado por uma conta:
# ativa primeiro, senao a mais recente (maior id na origem) -- mesma regra que
# antes vivia em ``agencias_transferegov.get_agencia_conta``, agora aplicada no
# consumo em vez de na ingestao, onde ela descartava dado.
_SQL_CONTAS_POR_PLANO = f"""
SELECT DISTINCT ON (banco.id_plano_acao)
    banco.id_plano_acao,
    banco.id_plano_acao_dado_bancario,
    banco.id_agencia_conta,
    banco.numero_agencia_plano_acao_dado_bancario,
    banco.numero_conta_plano_acao_dado_bancario,
    banco.situacao_conta_plano_acao_dado_bancario,
    banco.id_programa,
    banco.cod_ibge,
    programa.codigo_programa,
    programa.nome_programa
FROM {schemas.SCHEMA_TRANSFEREGOV}.{schemas.TABELA_PLANO_ACAO_DADO_BANCARIO} banco
LEFT JOIN {schemas.SCHEMA_TRANSFEREGOV}.{schemas.TABELA_PROGRAMA} programa
    ON banco.id_programa = programa.id_programa
WHERE banco.numero_agencia_plano_acao_dado_bancario IS NOT NULL
  AND banco.numero_conta_plano_acao_dado_bancario IS NOT NULL
ORDER BY
    banco.id_plano_acao,
    (banco.situacao_conta_plano_acao_dado_bancario = 'Conta Ativa') DESC,
    banco.id_plano_acao_dado_bancario DESC
"""


def _carregar_entes_transferegov() -> list[dict[str, Any]]:
    """Le agencia/conta de cada plano de acao de
    ``transferegov.plano_acao_dado_bancario_minc``.

    Antes esta DAG descobria agencia/conta chamando a API do Transferegov
    (uma requisicao por plano, ~18 mil) e guardava o resultado numa tabela
    que servia de cache. Agora quem extrai isso e
    ``api_plano_acao_dado_bancario_dag``, com todas as contas e todas as
    colunas -- aqui so se le o que ja esta no banco.
    """
    db = ClientPostgresDB(get_postgres_conn())
    linhas = db.execute_query(_SQL_CONTAS_POR_PLANO)

    entes = [
        {
            "id_plano_acao": id_plano_acao,
            "id_plano_acao_dado_bancario": id_dado_bancario,
            "id_agencia_conta": id_agencia_conta,
            "agencia": agencia,
            "conta": conta,
            "situacao_conta": situacao_conta,
            "id_programa": id_programa,
            "cod_ibge": cod_ibge,
            "codigo_programa": codigo_programa,
            "nome_programa": nome_programa,
        }
        for (
            id_plano_acao,
            id_dado_bancario,
            id_agencia_conta,
            agencia,
            conta,
            situacao_conta,
            id_programa,
            cod_ibge,
            codigo_programa,
            nome_programa,
        ) in linhas
    ]

    if not entes:
        raise ValueError(
            "[extracao_bbagil_dag] Nenhuma conta bancária encontrada em "
            f"{schemas.SCHEMA_TRANSFEREGOV}."
            f"{schemas.TABELA_PLANO_ACAO_DADO_BANCARIO} — rode "
            "api_plano_acao_dado_bancario_dag antes desta DAG"
        )

    logging.info(
        "[extracao_bbagil_dag] %d entes com agencia/conta lidos de %s.%s",
        len(entes),
        schemas.SCHEMA_TRANSFEREGOV,
        schemas.TABELA_PLANO_ACAO_DADO_BANCARIO,
    )
    return entes


def _combinacoes_extrato_pendentes(
    db: ClientPostgresDB, entes: list[dict[str, Any]], periodos: list[tuple[str, str]]
) -> list[Any]:
    feitas = db.execute_query(
        "SELECT id_plano_acao, periodo_inicial, periodo_final FROM "
        f"{schemas.SCHEMA_BBAGIL}.{schemas.TABELA_CONTROLE_EXTRATO} "
        "WHERE status IN ('ok', 'sem_dados')"
    )
    feitas_set = {
        (str(id_plano_acao), periodo_inicial, periodo_final)
        for id_plano_acao, periodo_inicial, periodo_final in feitas
    }

    combinacoes = [(ente, periodo) for ente in entes for periodo in periodos]
    return [
        (ente, periodo)
        for ente, periodo in combinacoes
        if (str(ente["id_plano_acao"]), periodo[0], periodo[1]) not in feitas_set
    ]


def _persistir_resultados_extrato(
    db: ClientPostgresDB, resultados: list[ResultadoItem]
) -> dict[str, int]:
    documentos_ok: list[dict[str, Any]] = []
    linhas_controle: list[dict[str, Any]] = []
    contagem = {"ok": 0, "sem_dados": 0, "erro": 0}

    for resultado in resultados:
        contagem[resultado.status] = contagem.get(resultado.status, 0) + 1
        ente, (periodo_inicial, periodo_final) = resultado.item
        linha_controle = {
            "id_plano_acao": ente["id_plano_acao"],
            "periodo_inicial": periodo_inicial,
            "periodo_final": periodo_final,
            "status": resultado.status,
            "qtd_transacoes": 0,
            "mensagem_erro": resultado.mensagem_erro,
        }

        if resultado.status == "ok":
            documentos_ok.append(resultado.dados)
            transacoes = resultado.dados.get("transactions") or []
            linha_controle["qtd_transacoes"] = len(transacoes)

        linhas_controle.append(linha_controle)

    linhas_raw = flatten_records(documentos_ok, record_key="transactions")

    if linhas_raw:
        db.insert_data(
            linhas_raw,
            table_name=schemas.TABELA_EXTRATO,
            primary_key=["id_plano_acao", "id"],
            conflict_fields=["id_plano_acao", "id"],
            schema=schemas.SCHEMA_BBAGIL,
        )
    if linhas_controle:
        db.insert_data(
            linhas_controle,
            table_name=schemas.TABELA_CONTROLE_EXTRATO,
            primary_key=["id_plano_acao", "periodo_inicial", "periodo_final"],
            conflict_fields=["id_plano_acao", "periodo_inicial", "periodo_final"],
            schema=schemas.SCHEMA_BBAGIL,
        )

    logging.info(
        "[extracao_bbagil_dag] Extrato BB Agil persistido no Postgres: %s "
        "(%d linhas raw)",
        contagem,
        len(linhas_raw),
    )
    return contagem


def _extrair_extrato(agencias_periodos: dict[str, Any]) -> dict[str, int]:
    entes = agencias_periodos["entes"]
    periodos = [tuple(p) for p in agencias_periodos["periodos"]]
    db = ClientPostgresDB(get_postgres_conn())

    pendentes = _combinacoes_extrato_pendentes(db, entes, periodos)
    logging.info(
        "[extracao_bbagil_dag] Extrato BB Agil: %d/%d combinacoes pendentes",
        len(pendentes),
        len(entes) * len(periodos),
    )

    contagem_total = {"ok": 0, "sem_dados": 0, "erro": 0}

    def _persistir_lote(resultados_lote: list[ResultadoItem]) -> None:
        for status, qtd in _persistir_resultados_extrato(db, resultados_lote).items():
            contagem_total[status] = contagem_total.get(status, 0) + qtd

    executar_lote(
        itens_pendentes=pendentes,
        chamar_api=_chamar_extrato,
        tratar_resposta_vazia=_marcar_extrato_sem_dados,
        tamanho_lote=TAMANHO_LOTE_PERSISTENCIA,
        ao_concluir_lote=_persistir_lote,
    )
    return contagem_total


def _subtransacoes_pendentes(db: ClientPostgresDB) -> list[dict[str, Any]]:
    # Candidatos: transacoes ja persistidas no extrato que tem
    # subtransactionquantity > 0 (regra de extracao da secao 7.2). As chaves
    # de integracao ja viajam na propria linha do extrato; o join com o dado
    # bancario e so para recuperar agencia e numero da conta, que a chamada
    # de sublancamento exige e a resposta nao devolve.
    candidatos = db.execute_query(
        "SELECT extrato.id_plano_acao, extrato.id, extrato.id_programa, "
        "extrato.cod_ibge, extrato.id_plano_acao_dado_bancario, "
        "extrato.id_agencia_conta, "
        "banco.numero_agencia_plano_acao_dado_bancario, "
        "banco.numero_conta_plano_acao_dado_bancario "
        f"FROM {schemas.SCHEMA_BBAGIL}.{schemas.TABELA_EXTRATO} extrato "
        f"JOIN {schemas.SCHEMA_TRANSFEREGOV}."
        f"{schemas.TABELA_PLANO_ACAO_DADO_BANCARIO} banco "
        "  ON extrato.id_plano_acao_dado_bancario = "
        "     banco.id_plano_acao_dado_bancario "
        "WHERE extrato.subtransactionquantity::int > 0"
    )
    feitas = db.execute_query(
        "SELECT id_plano_acao, id_transacao_pai FROM "
        f"{schemas.SCHEMA_BBAGIL}.{schemas.TABELA_CONTROLE_SUBTRANSACAO} "
        "WHERE status IN ('ok', 'sem_dados')"
    )
    feitas_set = {(str(id_plano_acao), str(id_pai)) for id_plano_acao, id_pai in feitas}

    return [
        {
            "id_plano_acao": id_plano_acao,
            "id": id_transacao,
            "id_programa": id_programa,
            "cod_ibge": cod_ibge,
            "id_plano_acao_dado_bancario": id_dado_bancario,
            "id_agencia_conta": id_agencia_conta,
            "agencia": agencia,
            "conta": conta,
        }
        for (
            id_plano_acao,
            id_transacao,
            id_programa,
            cod_ibge,
            id_dado_bancario,
            id_agencia_conta,
            agencia,
            conta,
        ) in candidatos
        if (str(id_plano_acao), str(id_transacao)) not in feitas_set
    ]


def _persistir_resultados_subtransacao(
    db: ClientPostgresDB, resultados: list[ResultadoItem]
) -> dict[str, int]:
    documentos_ok: list[dict[str, Any]] = []
    linhas_controle: list[dict[str, Any]] = []
    contagem = {"ok": 0, "sem_dados": 0, "erro": 0}

    for resultado in resultados:
        contagem[resultado.status] = contagem.get(resultado.status, 0) + 1
        item = resultado.item
        linha_controle = {
            "id_plano_acao": item["id_plano_acao"],
            "id_transacao_pai": item["id"],
            "status": resultado.status,
            "qtd_subtransacoes": 0,
            "mensagem_erro": resultado.mensagem_erro,
        }

        if resultado.status == "ok":
            documentos_ok.append(resultado.dados)
            linha_controle["qtd_subtransacoes"] = len(
                resultado.dados.get("subtransactions") or []
            )

        linhas_controle.append(linha_controle)

    linhas_raw = flatten_records(documentos_ok, record_key="subtransactions")

    if linhas_raw:
        db.insert_data(
            linhas_raw,
            table_name=schemas.TABELA_SUBTRANSACAO,
            primary_key=["id_plano_acao", "id_transacao_pai", "id"],
            conflict_fields=["id_plano_acao", "id_transacao_pai", "id"],
            schema=schemas.SCHEMA_BBAGIL,
        )
    if linhas_controle:
        db.insert_data(
            linhas_controle,
            table_name=schemas.TABELA_CONTROLE_SUBTRANSACAO,
            primary_key=["id_plano_acao", "id_transacao_pai"],
            conflict_fields=["id_plano_acao", "id_transacao_pai"],
            schema=schemas.SCHEMA_BBAGIL,
        )

    logging.info(
        "[extracao_bbagil_dag] Subtransacoes BB Agil persistidas no Postgres: "
        "%s (%d linhas raw)",
        contagem,
        len(linhas_raw),
    )
    return contagem


def _extrair_subtransacoes(_resumo_extrato: dict[str, int]) -> dict[str, int]:
    db = ClientPostgresDB(get_postgres_conn())

    pendentes = _subtransacoes_pendentes(db)
    logging.info(
        "[extracao_bbagil_dag] Subtransacoes BB Agil: %d pendentes", len(pendentes)
    )

    contagem_total = {"ok": 0, "sem_dados": 0, "erro": 0}

    def _persistir_lote(resultados_lote: list[ResultadoItem]) -> None:
        contagem_lote = _persistir_resultados_subtransacao(db, resultados_lote)
        for status, qtd in contagem_lote.items():
            contagem_total[status] = contagem_total.get(status, 0) + qtd

    executar_lote(
        itens_pendentes=pendentes,
        chamar_api=_chamar_subtransacao,
        tamanho_lote=TAMANHO_LOTE_PERSISTENCIA,
        ao_concluir_lote=_persistir_lote,
    )
    return contagem_total


@dag(
    dag_id="extracao_bbagil_dag",
    schedule=get_dynamic_schedule("extracao_bbagil_dag"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # DagRuns concorrentes desse DAG autenticam independentemente no SCA
    # com o mesmo client_id/secret -- o SCA invalida o token anterior
    # quando emite um novo pro mesmo client, entao 2+ runs em paralelo
    # derrubam o token uma da outra e tomam 401/403 sem relacao com o
    # throttle de requisicoes. So faz sentido 1 run ativa por vez.
    max_active_runs=1,
    default_args=default_args,
    tags=["minc", "pnab", "bsc", "bbagil", "raw"],
)
def extracao_bbagil_dag() -> None:
    """DAG de extracao financeira do BB Gestao Agil (BSC/SERPRO).

    Fluxo (Fase 1 do PNAB, adaptada para TaskFlow):

    1. ``carregar_contas_bancarias`` -- le agencia/conta de cada plano de
       acao de ``transferegov.plano_acao_dado_bancario_minc`` (uma conta por
       plano: ativa primeiro, senao a mais recente) e gera a lista de
       periodos mensais a extrair. Quem extrai as contas da API e
       ``api_plano_acao_dado_bancario_dag`` -- esta DAG so consome.
    2. ``extrair_extrato_bbagil`` -- para cada plano de acao x periodo
       pendente, chama o extrato via ``AsyncBscClient``. HTTP 400 "sem
       lancamentos" e registrado como dado de negocio, nao erro. Toda
       transacao retornada e persistida, uma linha por transacao, em
       ``bbagil.extrato_bbagil`` (upsert por ``id_plano_acao``+``id``);
       toda combinacao tentada (ok/sem_dados/erro) vira uma linha em
       ``bbagil.controle_extracao_bbagil_extrato``. A persistencia e feita
       a cada ``TAMANHO_LOTE_PERSISTENCIA`` itens (nao tudo no final): em
       listas de centenas de milhares de combinacoes/horas de execucao,
       gravar so no fim faria qualquer interrupcao perder o trabalho
       inteiro, sem visibilidade de progresso ate la. E essa tabela de
       controle, nao um arquivo em disco, que decide o que ja foi extraido
       (permite retomar sem reprocessar, e sem depender do filesystem de
       quem roda a DAG).
    3. ``extrair_subtransacoes_bbagil`` -- busca, via SQL direto em
       ``extrato_bbagil`` (join com ``plano_acao_dado_bancario_minc`` para
       agencia/conta), os sublancamentos das transacoes com
       ``subtransactionquantity`` > 0.
       Mesma logica de persistencia: ``bbagil.subtransacao_bbagil`` (upsert
       por ``id_plano_acao`` + ``id_transacao_pai`` + ``id`` -- o ``id`` da
       subtransacao e sequencial por transacao-mae, nao um identificador
       global) e controle em
       ``bbagil.controle_extracao_bbagil_subtransacoes``.

    Esta DAG so extrai e deposita o dado bruto no Postgres; nao gera mais
    ``fato_bbagil`` nem nenhum arquivo local. Os filtros de negocio (o que
    entrava no ``fato_bbagil``) e a agregacao final, que rodavam aqui em
    pandas, foram removidos daqui -- a prioridade agora e completar a
    extracao (as tabelas raw acima). A camada de transformacao (dbt ou
    outra) sobre essas tabelas raw fica para depois, ainda nao
    implementada.

    Toda a complexidade de HTTP/concorrencia/retry fica em ``cliente_bsc``
    e ``execucao_assincrona_bsc``; a logica de cada passo fica nas funcoes
    ``_extrair_*``/``_persistir_*`` no topo do modulo. Esta DAG so orquestra.
    """

    @task
    def carregar_contas_bancarias() -> dict[str, Any]:
        return {
            "entes": _carregar_entes_transferegov(),
            "periodos": gerar_periodos_mensais(),
        }

    @task
    def extrair_extrato_bbagil(agencias_periodos: dict[str, Any]) -> dict[str, int]:
        return _extrair_extrato(agencias_periodos)

    @task
    def extrair_subtransacoes_bbagil(resumo_extrato: dict[str, int]) -> dict[str, int]:
        return _extrair_subtransacoes(resumo_extrato)

    agencias_periodos = carregar_contas_bancarias()
    resumo_extrato = extrair_extrato_bbagil(agencias_periodos)
    extrair_subtransacoes_bbagil(resumo_extrato)


extracao_bbagil_dag()
