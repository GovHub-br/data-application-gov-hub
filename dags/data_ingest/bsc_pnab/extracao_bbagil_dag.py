import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.models import Variable
from airflow.sdk import dag, task

import agencias_transferegov
import config_bsc_pnab as settings
from agencias_bbagil import gerar_periodos_mensais
from cliente_bsc import AsyncBscClient, BscRequestError, is_empty_extrato_response
from cliente_postgres import ClientPostgresDB
from execucao_assincrona_bsc import ResultadoItem, executar_lote
from file_io_local import flatten_records
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

_SCHEMA_TRANSFEREGOV = "transferegov_fundo_a_fundo"
_SCHEMA_BSC_PNAB = "bsc_pnab"

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


def _json_nativo(registros: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Converte escalares numpy (int64/float64, comuns em
    ``DataFrame.to_dict("records")``) para tipos nativos do Python.

    Necessario porque o XCom do Airflow serializa em JSON, e ``json.dumps``
    nao sabe lidar com ``numpy.int64`` -- sem isso, a task quebra so ao
    tentar devolver o resultado, nao na logica em si.
    """
    return [
        {k: (v.item() if hasattr(v, "item") else v) for k, v in registro.items()}
        for registro in registros
    ]


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
    # nao global).
    resposta["id_plano_acao"] = item["id_plano_acao"]
    resposta["id_transacao_pai"] = item["id"]
    return resposta


def _carregar_entes_transferegov() -> list[dict[str, Any]]:
    """Descobre agencia/conta de cada plano de acao, buscando na API do
    Transferegov so o que ainda nao esta em
    ``raw_planos_acao_dado_bancario`` -- essa tabela e uma linha por
    ``id_plano_acao`` ja resolvido em execucoes anteriores, entao ela
    funciona como cache: sem isso, toda execucao re-buscaria agencia/conta
    de TODOS os planos de acao (uma chamada HTTP por plano, caro em volumes
    grandes como o atual, ~18 mil planos)."""
    codigos_programas = Variable.get("transferegov_programas_ids", deserialize_json=True)
    db = ClientPostgresDB(get_postgres_conn())

    linhas_conhecidas = db.execute_query(
        "SELECT id_plano_acao, agencia, conta, situacao_conta, id_programa, "
        "codigo_programa, nome_programa FROM "
        f"{_SCHEMA_TRANSFEREGOV}.raw_planos_acao_dado_bancario"
    )
    entes_conhecidos = [
        {
            "id_plano_acao": id_plano_acao,
            "agencia": agencia,
            "conta": conta,
            "situacao_conta": situacao_conta,
            "id_programa": id_programa,
            "codigo_programa": codigo_programa,
            "nome_programa": nome_programa,
        }
        for (
            id_plano_acao,
            agencia,
            conta,
            situacao_conta,
            id_programa,
            codigo_programa,
            nome_programa,
        ) in linhas_conhecidas
    ]
    ids_conhecidos = {str(ente["id_plano_acao"]) for ente in entes_conhecidos}

    logger = agencias_transferegov.configure_logger(
        log_dir=settings.TRANSFEREGOV_LOG_DIR, log_to_file=True
    )
    novos_brutos = agencias_transferegov.get_contas_agencias_programas(
        codigos_programas=codigos_programas,
        logger=logger,
        ids_plano_acao_conhecidos=ids_conhecidos,
    )
    novos_validos = [
        registro
        for registro in novos_brutos
        if registro.get("agencia") is not None and registro.get("conta") is not None
    ]

    entes = _json_nativo(entes_conhecidos + novos_validos)

    logging.info(
        "[extracao_bbagil_dag] %d entes com agencia/conta (%d ja conhecidos no "
        "Postgres + %d novos validos de %d planos novos retornados pela API)",
        len(entes),
        len(entes_conhecidos),
        len(novos_validos),
        len(novos_brutos),
    )
    return entes


def _persistir_agencias_contas(entes: list[dict[str, Any]]) -> None:
    """Persiste a descoberta de agencia/conta (Transferegov) no Postgres.

    Mesmo padrao das demais DAGs do Transferegov (``api_planos_acao_dag`` etc):
    upsert por ``id_plano_acao`` no schema ``transferegov_fundo_a_fundo``, para
    que a descoberta de agencia/conta fique auditavel e consultavel fora do
    XCom (que e efemero e some depois que a DAG run e limpa).
    """
    db = ClientPostgresDB(get_postgres_conn())
    db.insert_data(
        entes,
        table_name="raw_planos_acao_dado_bancario",
        primary_key=["id_plano_acao"],
        conflict_fields=["id_plano_acao"],
        schema=_SCHEMA_TRANSFEREGOV,
    )
    logging.info(
        "[extracao_bbagil_dag] %d registros de agencia/conta persistidos em "
        "%s.raw_planos_acao_dado_bancario",
        len(entes),
        _SCHEMA_TRANSFEREGOV,
    )


def _combinacoes_extrato_pendentes(
    db: ClientPostgresDB, entes: list[dict[str, Any]], periodos: list[tuple[str, str]]
) -> list[Any]:
    feitas = db.execute_query(
        f"SELECT id_plano_acao, periodo_inicial, periodo_final FROM {_SCHEMA_BSC_PNAB}"
        ".controle_extracao_bbagil_extrato WHERE status IN ('ok', 'sem_dados')"
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
            table_name="raw_bbagil_extrato_transacoes",
            primary_key=["id_plano_acao", "id"],
            conflict_fields=["id_plano_acao", "id"],
            schema=_SCHEMA_BSC_PNAB,
        )
    if linhas_controle:
        db.insert_data(
            linhas_controle,
            table_name="controle_extracao_bbagil_extrato",
            primary_key=["id_plano_acao", "periodo_inicial", "periodo_final"],
            conflict_fields=["id_plano_acao", "periodo_inicial", "periodo_final"],
            schema=_SCHEMA_BSC_PNAB,
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
    # Candidatos: transacoes ja persistidas no raw do extrato que tem
    # subtransactionquantity > 0, com agencia/conta recuperados via join com
    # a descoberta Transferegov (a resposta de subtransacao nao devolve
    # agencia/conta no corpo).
    candidatos = db.execute_query(
        f"SELECT r.id_plano_acao, r.id, p.agencia, p.conta "
        f"FROM {_SCHEMA_BSC_PNAB}.raw_bbagil_extrato_transacoes r "
        f"JOIN {_SCHEMA_TRANSFEREGOV}.raw_planos_acao_dado_bancario p "
        f"  ON r.id_plano_acao = p.id_plano_acao "
        f"WHERE r.subtransactionquantity::int > 0"
    )
    feitas = db.execute_query(
        f"SELECT id_plano_acao, id_transacao_pai FROM {_SCHEMA_BSC_PNAB}"
        ".controle_extracao_bbagil_subtransacoes WHERE status IN ('ok', 'sem_dados')"
    )
    feitas_set = {(str(id_plano_acao), str(id_pai)) for id_plano_acao, id_pai in feitas}

    return [
        {"id_plano_acao": id_plano_acao, "id": id_transacao, "agencia": agencia, "conta": conta}
        for id_plano_acao, id_transacao, agencia, conta in candidatos
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
            table_name="raw_bbagil_subtransacoes",
            primary_key=["id_plano_acao", "id_transacao_pai", "id"],
            conflict_fields=["id_plano_acao", "id_transacao_pai", "id"],
            schema=_SCHEMA_BSC_PNAB,
        )
    if linhas_controle:
        db.insert_data(
            linhas_controle,
            table_name="controle_extracao_bbagil_subtransacoes",
            primary_key=["id_plano_acao", "id_transacao_pai"],
            conflict_fields=["id_plano_acao", "id_transacao_pai"],
            schema=_SCHEMA_BSC_PNAB,
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

    1. ``extrair_agencias_transferegov`` -- descobre agencia/conta de cada
       plano de acao via API oficial do Transferegov
       (``programa`` -> ``plano_acao`` -> ``plano_acao_dado_bancario``, em
       ``agencias_transferegov.py``) e gera a lista de periodos mensais a
       extrair.
    1b. ``persistir_agencias_contas_transferegov`` -- salva a descoberta de
       agencia/conta em ``transferegov_fundo_a_fundo.raw_planos_acao_dado_bancario``
       (upsert por ``id_plano_acao``), em paralelo com o passo 2, para nao
       depender so do XCom (efemero) para auditar essa informacao.
    2. ``extrair_extrato_bbagil`` -- para cada plano de acao x periodo
       pendente, chama o extrato via ``AsyncBscClient``. HTTP 400 "sem
       lancamentos" e registrado como dado de negocio, nao erro. Toda
       transacao retornada e persistida, uma linha por transacao, em
       ``bsc_pnab.raw_bbagil_extrato_transacoes`` (upsert por
       ``id_plano_acao``+``id``);
       toda combinacao tentada (ok/sem_dados/erro) vira uma linha em
       ``bsc_pnab.controle_extracao_bbagil_extrato``. A persistencia e feita
       a cada ``TAMANHO_LOTE_PERSISTENCIA`` itens (nao tudo no final): em
       listas de centenas de milhares de combinacoes/horas de execucao,
       gravar so no fim faria qualquer interrupcao perder o trabalho
       inteiro, sem visibilidade de progresso ate la. E essa tabela de
       controle, nao um arquivo em disco, que decide o que ja foi extraido
       (permite retomar sem reprocessar, e sem depender do filesystem de
       quem roda a DAG).
    3. ``extrair_subtransacoes_bbagil`` -- busca, via SQL direto em
       ``raw_bbagil_extrato_transacoes`` (join com
       ``raw_planos_acao_dado_bancario`` para agencia/conta), os
       sublancamentos das transacoes com ``subtransactionquantity`` > 0.
       Mesma logica de persistencia: raw em
       ``bsc_pnab.raw_bbagil_subtransacoes`` (upsert por ``id_plano_acao`` +
       ``id_transacao_pai`` + ``id`` -- o ``id`` da subtransacao e
       sequencial por transacao-mae, nao um identificador global) e
       controle em ``bsc_pnab.controle_extracao_bbagil_subtransacoes``.

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
    def extrair_agencias_transferegov() -> dict[str, Any]:
        return {
            "entes": _carregar_entes_transferegov(),
            "periodos": gerar_periodos_mensais(),
        }

    @task
    def persistir_agencias_contas_transferegov(agencias_periodos: dict[str, Any]) -> None:
        _persistir_agencias_contas(agencias_periodos["entes"])

    @task
    def extrair_extrato_bbagil(agencias_periodos: dict[str, Any]) -> dict[str, int]:
        return _extrair_extrato(agencias_periodos)

    @task
    def extrair_subtransacoes_bbagil(resumo_extrato: dict[str, int]) -> dict[str, int]:
        return _extrair_subtransacoes(resumo_extrato)

    agencias_periodos = extrair_agencias_transferegov()
    persistir_agencias_contas_transferegov(agencias_periodos)
    resumo_extrato = extrair_extrato_bbagil(agencias_periodos)
    extrair_subtransacoes_bbagil(resumo_extrato)


extracao_bbagil_dag()
