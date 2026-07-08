import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.models import Variable
from airflow.sdk import dag, task

import agencias_transferegov
import config_bsc_pnab as settings
import regras_negocio_bbagil as regras
from agencias_bbagil import gerar_periodos_mensais
from cliente_bsc import AsyncBscClient, BscRequestError, is_empty_extrato_response
from cliente_postgres import ClientPostgresDB
from execucao_assincrona_bsc import executar_lote
from file_io_local import flatten_json_dir_to_dataframe, save_dataframe
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

_SCHEMA_TRANSFEREGOV = "transferegov_fundo_a_fundo"
_SCHEMA_BSC_PNAB = "bsc_pnab"

default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
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


def _caminho_extrato(ente: dict[str, Any], periodo: tuple[str, str]) -> Path:
    ano_mes = periodo[0][:7]  # YYYY-MM
    return (
        settings.BBAGIL_EXTRATO_DIR
        / f"plano_{ente['id_plano_acao']}"
        / f"bbagil_extrato_{ano_mes}.json"
    )


def _marcar_extrato_sem_dados(exc: BscRequestError) -> dict[str, Any] | None:
    if is_empty_extrato_response(exc):
        return {"status": "sem_dados", "erro": exc.response_text}
    return None


async def _chamar_extrato(client: AsyncBscClient, item: Any) -> Any:
    # A resposta do BSC traz agencia/conta/saldo/transactions, mas nao
    # identifica o ente/plano de acao de origem -- injeta aqui para que
    # flatten_json_dir_to_dataframe propague esses campos para cada
    # transacao (usados nos filtros e no agrupamento do fato).
    ente, (periodo_inicial, periodo_final) = item
    resposta = await client.bbagil_extrato_orgao_controle(
        agencia=int(ente["agencia"]),
        numero_conta=int(ente["conta"]),
        periodo_inicial=periodo_inicial,
        periodo_final=periodo_final,
    )
    resposta[regras.COL_ENTE] = ente["id_plano_acao"]
    resposta["id_programa"] = ente.get("id_programa")
    resposta["codigo_programa"] = ente.get("codigo_programa")
    resposta["nome_programa"] = ente.get("nome_programa")
    return resposta


def _carregar_entes_transferegov() -> list[dict[str, Any]]:
    codigos_programas = Variable.get("transferegov_programas_ids", deserialize_json=True)

    logger = agencias_transferegov.configure_logger(
        log_dir=settings.TRANSFEREGOV_LOG_DIR, log_to_file=True
    )
    contas = agencias_transferegov.get_contas_agencias_programas(
        codigos_programas=codigos_programas,
        logger=logger,
    )

    registros = [
        registro
        for registro in contas
        if registro.get("agencia") is not None and registro.get("conta") is not None
    ]
    entes = _json_nativo(registros)

    logging.info(
        "[extracao_bbagil_dag] %d planos de acao com agencia/conta via "
        "Transferegov (de %d totais retornados pela API)",
        len(entes),
        len(contas),
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


def _extrair_extrato(agencias_periodos: dict[str, Any]) -> dict[str, int]:
    entes = agencias_periodos["entes"]
    periodos = [tuple(p) for p in agencias_periodos["periodos"]]

    combinacoes = [(ente, periodo) for ente in entes for periodo in periodos]
    pendentes = [item for item in combinacoes if not _caminho_extrato(*item).exists()]
    logging.info(
        "[extracao_bbagil_dag] Extrato BB Agil: %d/%d combinacoes pendentes",
        len(pendentes),
        len(combinacoes),
    )

    return executar_lote(
        itens_pendentes=pendentes,
        chamar_api=_chamar_extrato,
        caminho_saida=lambda item: _caminho_extrato(*item),
        tratar_resposta_vazia=_marcar_extrato_sem_dados,
    )


def _consolidar_extrato() -> dict[str, str]:
    df_bruto = flatten_json_dir_to_dataframe(
        settings.BBAGIL_EXTRATO_DIR, record_key="transactions"
    )
    caminho_bruto = settings.BBAGIL_CONSOLIDADO_DIR / "bbagil_extrato_bruto.parquet"
    save_dataframe(df_bruto, caminho_bruto)

    df_filtrado = regras.pipeline_filtro_extrato(df_bruto)
    caminho_filtrado = settings.BBAGIL_CONSOLIDADO_DIR / "bbagil_extrato_filtrado.parquet"
    save_dataframe(df_filtrado, caminho_filtrado)

    return {"bruto": str(caminho_bruto), "filtrado": str(caminho_filtrado)}


def _caminho_subtransacao(linha: pd.Series) -> Path:
    return (
        settings.BBAGIL_SUBTRANSACOES_DIR
        / f"plano_{linha[regras.COL_ENTE]}"
        / f"bbagil_subtransacoes_{linha[regras.COL_ID_TRANSACTION]}.json"
    )


async def _chamar_subtransacao(client: AsyncBscClient, item: pd.Series) -> Any:
    resposta = await client.bbagil_extrato_sub_lancamentos_orgao_controle(
        # O extrato bruto flatten guarda os campos de raiz da resposta do
        # BSC como vieram (agencia, conta) -- nao "numero_conta".
        agencia=str(item["agencia"]),
        numero_conta=str(item["conta"]),
        id_transaction=str(item[regras.COL_ID_TRANSACTION]),
    )
    # Mesma injecao do extrato: a resposta nao identifica o ente/plano de
    # acao, que e necessario para o filtro/agrupamento por ente.
    resposta[regras.COL_ENTE] = item[regras.COL_ENTE]
    return resposta


def _extrair_subtransacoes(caminhos_extrato: dict[str, str]) -> dict[str, int]:
    # Le do parquet BRUTO (pre-filtro): o filtro 2 do extrato descarta
    # justamente as transacoes com subTransactionQuantity > 0 (elas sao
    # representadas pelos sublancamentos, nao pelo registro-pai), entao o
    # parquet filtrado nunca teria candidatos a subtransacao.
    df_extrato_bruto = pd.read_parquet(caminhos_extrato["bruto"])
    coluna_qtd = regras.COL_SUBTRANSACTION_QTD
    if coluna_qtd in df_extrato_bruto.columns:
        pendentes_transacoes = df_extrato_bruto[df_extrato_bruto[coluna_qtd] > 0]
    else:
        pendentes_transacoes = df_extrato_bruto.iloc[0:0]

    itens = [linha for _, linha in pendentes_transacoes.iterrows()]
    pendentes = [item for item in itens if not _caminho_subtransacao(item).exists()]
    logging.info(
        "[extracao_bbagil_dag] Subtransacoes BB Agil: %d/%d pendentes",
        len(pendentes),
        len(itens),
    )

    return executar_lote(
        itens_pendentes=pendentes,
        chamar_api=_chamar_subtransacao,
        caminho_saida=_caminho_subtransacao,
    )


def _consolidar_subtransacoes() -> str:
    df_bruto = flatten_json_dir_to_dataframe(
        settings.BBAGIL_SUBTRANSACOES_DIR, record_key="subtransactions"
    )
    df_filtrado = regras.pipeline_filtro_subtransacoes(df_bruto)
    caminho = settings.BBAGIL_CONSOLIDADO_DIR / "bbagil_subtransacoes_filtrado.parquet"
    save_dataframe(df_filtrado, caminho)
    return str(caminho)


def _construir_fato(caminhos_extrato: dict[str, str], caminho_subtransacoes: str) -> str:
    df_extrato = pd.read_parquet(caminhos_extrato["filtrado"])
    df_subtransacoes = pd.read_parquet(caminho_subtransacoes)
    fato = regras.montar_fato_bbagil(df_extrato, df_subtransacoes)
    return str(save_dataframe(fato, settings.FATO_BBAGIL_PATH))


def _persistir_fato_bbagil(caminho_fato: str) -> None:
    """Carrega o fato_bbagil.parquet no Postgres.

    O parquet continua existindo como checkpoint/staging (grao final de um
    pipeline com milhares de chamadas HTTP individuais ao BSC, caro demais
    para reprocessar a cada retry) -- esta task so espelha o resultado ja
    consolidado no banco, upsert por (ente_bbagil, documento_beneficiario_bbagil)
    -- a mesma chave usada no groupby de ``montar_fato_bbagil`` -- para ficar
    consultavel fora do filesystem do Airflow.
    """
    df_fato = pd.read_parquet(caminho_fato)
    registros = _json_nativo(df_fato.to_dict("records"))

    db = ClientPostgresDB(get_postgres_conn())
    db.insert_data(
        registros,
        table_name="fato_bbagil",
        primary_key=["ente_bbagil", "documento_beneficiario_bbagil"],
        conflict_fields=["ente_bbagil", "documento_beneficiario_bbagil"],
        schema=_SCHEMA_BSC_PNAB,
    )
    logging.info(
        "[extracao_bbagil_dag] %d linhas do fato_bbagil persistidas em %s.fato_bbagil",
        len(registros),
        _SCHEMA_BSC_PNAB,
    )


@dag(
    dag_id="extracao_bbagil_dag",
    schedule=get_dynamic_schedule("extracao_bbagil_dag"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "pnab", "bsc", "bbagil", "raw"],
)
def extracao_bbagil_dag() -> None:
    """DAG de extracao e consolidacao financeira do BB Gestao Agil (BSC/SERPRO).

    Fluxo (Fases 1-2 do PNAB, adaptadas para TaskFlow):

    1. ``extrair_agencias_transferegov`` -- descobre agencia/conta de cada
       plano de acao via API oficial do Transferegov
       (``programa`` -> ``plano_acao`` -> ``plano_acao_dado_bancario``, em
       ``agencias_transferegov.py``) e gera a lista de periodos mensais a
       extrair. Substitui a planilha Excel legada, que falhava em silencio
       quando o arquivo nao estava mapeado no ambiente.
    1b. ``persistir_agencias_contas_transferegov`` -- salva a descoberta de
       agencia/conta em ``transferegov_fundo_a_fundo.raw_planos_acao_dado_bancario``
       (upsert por ``id_plano_acao``), em paralelo com o passo 2, para nao
       depender so do XCom (efemero) para auditar essa informacao.
    2. ``extrair_extrato_bbagil`` -- para cada plano de acao x periodo
       pendente (checkpoint por arquivo, sem timestamp no nome), chama o
       extrato via ``AsyncBscClient``. HTTP 400 "sem lancamentos" e
       registrado como dado de negocio, nao erro.
    3. ``consolidar_extrato_bbagil`` -- achata os JSONs brutos em Parquet
       (mantendo uma copia bruta -- necessaria no passo 4, ja que o filtro 2
       do extrato descarta justamente as transacoes com subtransacoes) e
       aplica os 8 filtros de negocio (``pipeline_filtro_extrato``) numa
       segunda copia, filtrada, que alimenta o fato.
    4. ``extrair_subtransacoes_bbagil`` -- busca, a partir do parquet BRUTO,
       os sublancamentos das transacoes com ``subTransactionQuantity`` > 0.
    5. ``consolidar_subtransacoes_bbagil`` -- achata e aplica os 5 filtros
       de negocio (``pipeline_filtro_subtransacoes``).
    6. ``construir_fato_bbagil`` -- une extrato filtrado + subtransacoes
       filtradas, agrupa por (ente, beneficiario) e aplica o limiar de R$375.
    7. ``persistir_fato_bbagil_postgres`` -- carrega o fato final em
       ``bsc_pnab.fato_bbagil`` (upsert por ente_bbagil + documento do
       beneficiario). O parquet continua sendo o checkpoint/staging do
       pipeline; esta task so espelha o resultado consolidado no banco.

    Nota: como a API do Transferegov nao devolve o nome do ente/municipio
    no dado bancario (so ``id_plano_acao``), o ``fato_bbagil`` identifica
    cada ente por ``id_plano_acao`` -- nao por nome. Se for necessario um
    nome legivel, ``agencias_transferegov.get_ids_plano_acao()`` ja busca
    ``nome_ente_recebedor_plano_acao``/``uf_ente_recebedor_plano_acao``,
    mas ``get_contas_agencias_programas()`` nao propaga esses campos para a
    lista final.

    Toda a complexidade de HTTP/concorrencia/retry fica em ``cliente_bsc``
    e ``execucao_assincrona_bsc``; toda a logica de negocio fica em
    ``regras_negocio_bbagil``; a logica de cada passo fica nas funcoes
    ``_extrair_*``/``_consolidar_*``/``_construir_fato`` no topo do modulo.
    Esta DAG so orquestra.
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
    def consolidar_extrato_bbagil(_resumo_extracao: dict[str, int]) -> dict[str, str]:
        return _consolidar_extrato()

    @task
    def extrair_subtransacoes_bbagil(caminhos_extrato: dict[str, str]) -> dict[str, int]:
        return _extrair_subtransacoes(caminhos_extrato)

    @task
    def consolidar_subtransacoes_bbagil(_resumo_subtransacoes: dict[str, int]) -> str:
        return _consolidar_subtransacoes()

    @task
    def construir_fato_bbagil(
        caminhos_extrato: dict[str, str], caminho_subtransacoes: str
    ) -> str:
        return _construir_fato(caminhos_extrato, caminho_subtransacoes)

    @task
    def persistir_fato_bbagil_postgres(caminho_fato: str) -> None:
        _persistir_fato_bbagil(caminho_fato)

    agencias_periodos = extrair_agencias_transferegov()
    persistir_agencias_contas_transferegov(agencias_periodos)
    resumo_extrato = extrair_extrato_bbagil(agencias_periodos)
    caminhos_extrato = consolidar_extrato_bbagil(resumo_extrato)
    resumo_subtransacoes = extrair_subtransacoes_bbagil(caminhos_extrato)
    caminho_subtransacoes = consolidar_subtransacoes_bbagil(resumo_subtransacoes)
    caminho_fato = construir_fato_bbagil(caminhos_extrato, caminho_subtransacoes)
    persistir_fato_bbagil_postgres(caminho_fato)


extracao_bbagil_dag()
