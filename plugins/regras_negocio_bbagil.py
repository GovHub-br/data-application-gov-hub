"""Regras de negocio da Fase 2 do PNAB (consolidacao financeira BB Agil).

Mantido separado da DAG (isolamento de responsabilidade: a DAG so orquestra
``@task``s, a logica de transformacao pandas vive aqui e e testavel isolada).

Os nomes de campo abaixo (``beneficiaryDocumentId``, ``valueDate``,
``creditDebitIndicator`` etc.) seguem o contrato documentado no guia de
engenharia reversa do BSC/PNAB. Se o payload real do BSC usar nomes
diferentes, ajuste as constantes ``COL_*`` abaixo -- e o unico lugar que
precisa mudar.
"""

import logging
from typing import Optional

import pandas as pd

import config_bsc_pnab as settings

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Nomes de campo (ajustar aqui se o contrato real do BSC divergir)
# --------------------------------------------------------------------------
COL_BENEFICIARIO_DOC = "beneficiaryDocumentId"
COL_BENEFICIARIO_NOME = "beneficiaryName"
COL_SUBTRANSACTION_QTD = "subTransactionQuantity"
COL_DESCRICAO = "descriptionName"
COL_CREDITO_DEBITO = "creditDebitIndicator"
COL_VALUE_DATE = "valueDate"  # extrato, formato DD/MM/YYYY
COL_PAYMENT_DATE = "paymentDate"  # subtransacoes, formato DD/MM/YYYY
COL_VALOR_EXTRATO = "valor"
COL_VALOR_SUBTRANSACAO = "value"
COL_ACCOUNTABILITY = "subtransactionAccountabilityName"
COL_ENTE = "ente"
COL_ID_TRANSACTION = "id"

EXTRATO_TRANSACAO_EXCLUIR = [
    "BB-APLIC C.PRZ-APL.AUT",
    "Resgate Automatico",
    "Impostos",
    "ORDEM BANC CANCELADA",
    "Ordem Bancaria",
    "Resgate BB Fix",
    "CREDITO CONVENIO",
    "Estorno Resgate Automatico",
]

PREFIXOS_ENTE_PUBLICO = ("MUNICIPIO", "ESTADO", "FUNDO", "SECRETARIA", "SEFAZ")


def _parse_data_br(serie: pd.Series) -> pd.Series:
    """Converte data no formato DD/MM/YYYY para datetime, tolerando NaT."""
    return pd.to_datetime(serie, format="%d/%m/%Y", errors="coerce")


def aplicar_corte_temporal(
    df: pd.DataFrame, coluna_data: str, data_corte: str = settings.DATA_CORTE_PNAB
) -> pd.DataFrame:
    """Remove transacoes com data posterior ao corte do Ciclo 1 do PNAB."""
    if df.empty or coluna_data not in df.columns:
        return df
    datas = _parse_data_br(df[coluna_data])
    corte = pd.to_datetime(data_corte)
    antes = len(df)
    df_filtrado = df[datas <= corte].copy()
    logger.info(
        "[regras_negocio_bbagil] Corte temporal (<=%s): %d -> %d linhas",
        data_corte,
        antes,
        len(df_filtrado),
    )
    return df_filtrado


def _filtrar_repasses_entes_publicos(df: pd.DataFrame, coluna_nome: str) -> pd.DataFrame:
    if df.empty or coluna_nome not in df.columns:
        return df
    nomes = df[coluna_nome].fillna("").str.upper()
    mascara_repasse = nomes.str.startswith(PREFIXOS_ENTE_PUBLICO)
    antes = len(df)
    df_filtrado = df[~mascara_repasse].copy()
    logger.info(
        "[regras_negocio_bbagil] Remocao de repasses entre entes publicos: %d -> %d",
        antes,
        len(df_filtrado),
    )
    return df_filtrado


def _marcar_pares_credito_debito(df: pd.DataFrame) -> pd.Series:
    """Identifica pares credito/debito do mesmo ente+documento+valor a
    remover: estorno no mesmo dia (5a) ou devolucao futura de debito
    anterior (5b)."""
    chave = [COL_ENTE, COL_BENEFICIARIO_DOC, COL_VALOR_EXTRATO]
    if not set(chave).issubset(df.columns):
        return pd.Series(False, index=df.index)

    debitos = df[df[COL_CREDITO_DEBITO] == "D"]
    creditos = df[df[COL_CREDITO_DEBITO] == "C"]

    marcados = pd.Series(False, index=df.index)

    for chave_valores, grupo_debitos in debitos.groupby(chave):
        grupo_creditos = creditos[
            (creditos[COL_ENTE] == chave_valores[0])
            & (creditos[COL_BENEFICIARIO_DOC] == chave_valores[1])
            & (creditos[COL_VALOR_EXTRATO] == chave_valores[2])
        ]
        if grupo_creditos.empty:
            continue
        # 5a: mesma data (estorno) ou 5b: credito em data posterior a
        # qualquer debito do grupo (devolucao futura).
        marcados.loc[grupo_debitos.index] = True
        marcados.loc[grupo_creditos.index] = True

    return marcados


def pipeline_filtro_extrato(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica os 8 filtros sequenciais do extrato BB Agil, na ordem
    documentada. Recebe o DataFrame achatado (1 linha por transacao)."""
    if df.empty:
        return df

    total_inicial = len(df)

    # 1. Corte temporal (aplicado cedo para reduzir volume dos passos seguintes).
    df = aplicar_corte_temporal(df, COL_VALUE_DATE)

    # 2. Beneficiario valido e sem subtransacoes (as com subtransacoes sao
    #    tratadas via o extrato de sublancamentos, para nao contar 2x).
    if {COL_BENEFICIARIO_DOC, COL_SUBTRANSACTION_QTD}.issubset(df.columns):
        df = df[
            (df[COL_BENEFICIARIO_DOC] != "0") & (df[COL_SUBTRANSACTION_QTD] == 0)
        ].copy()

    # 3. Remove transferencias para o proprio Banco do Brasil.
    if COL_BENEFICIARIO_DOC in df.columns:
        df = df[df[COL_BENEFICIARIO_DOC] != settings.BB_BENEFICIARY_DOCUMENT_ID].copy()

    # 4. Remove transacoes internas / impostos.
    if COL_DESCRICAO in df.columns:
        df = df[~df[COL_DESCRICAO].isin(EXTRATO_TRANSACAO_EXCLUIR)].copy()

    # 5. Remove devolucoes de saldo ao Fundo Nacional de Cultura.
    if COL_BENEFICIARIO_DOC in df.columns:
        df = df[df[COL_BENEFICIARIO_DOC] != settings.FNC_CNPJ].copy()

    # 6. Detecta e remove pares credito/debito (estorno ou devolucao futura).
    if COL_CREDITO_DEBITO in df.columns:
        pares = _marcar_pares_credito_debito(df)
        df = df[~pares].copy()

        # 7. Mantem apenas debitos (creditos remanescentes tambem sao descartados).
        df = df[df[COL_CREDITO_DEBITO] == "D"].copy()

    # 8. Remove repasses entre entes publicos.
    df = _filtrar_repasses_entes_publicos(df, COL_BENEFICIARIO_NOME)

    logger.info(
        "[regras_negocio_bbagil] pipeline_filtro_extrato: %d -> %d linhas",
        total_inicial,
        len(df),
    )
    return df


def pipeline_filtro_subtransacoes(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica os 5 filtros sequenciais das subtransacoes BB Agil."""
    if df.empty:
        return df

    total_inicial = len(df)

    # 1. Remove sem beneficiario.
    if COL_BENEFICIARIO_DOC in df.columns:
        df = df[df[COL_BENEFICIARIO_DOC] != "0"].copy()

    # 2. Mantem apenas pagas.
    if COL_ACCOUNTABILITY in df.columns:
        df = df[df[COL_ACCOUNTABILITY] == "Pago"].copy()

    # 3. Normaliza para valor absoluto.
    if COL_VALOR_SUBTRANSACAO in df.columns:
        df[COL_VALOR_SUBTRANSACAO] = df[COL_VALOR_SUBTRANSACAO].abs()

    # 4. Corte temporal.
    df = aplicar_corte_temporal(df, COL_PAYMENT_DATE)

    # 5. Remove repasses entre entes publicos.
    df = _filtrar_repasses_entes_publicos(df, COL_BENEFICIARIO_NOME)

    logger.info(
        "[regras_negocio_bbagil] pipeline_filtro_subtransacoes: %d -> %d linhas",
        total_inicial,
        len(df),
    )
    return df


def remover_transacoes_ciclo2(
    df_extrato: pd.DataFrame, df_entes_ciclo2: Optional[pd.DataFrame]
) -> pd.DataFrame:
    """Remove do extrato as transacoes cuja agencia+conta de destino batem
    com contas do Ciclo 2 (administradas separadamente, nao devem ser
    contabilizadas no PNAB Ciclo 1).

    ``df_entes_ciclo2`` e opcional: se nao for fornecido (planilha de
    referencia ainda nao carregada), o passo e pulado com um aviso -- e
    preferivel nao filtrar a filtrar errado por falta do arquivo.
    """
    if df_entes_ciclo2 is None or df_entes_ciclo2.empty:
        logger.warning(
            "[regras_negocio_bbagil] Sem referencia de entes do Ciclo 2 -- "
            "pulando remocao (verifique CNPJs e IBGE dos Entes.xlsx)"
        )
        return df_extrato

    chaves_ciclo2 = set(zip(df_entes_ciclo2["agencia"], df_entes_ciclo2["numero_conta"]))
    mascara = df_extrato.apply(
        lambda linha: (linha.get("agencia"), linha.get("numero_conta")) in chaves_ciclo2,
        axis=1,
    )
    antes = len(df_extrato)
    df_filtrado = df_extrato[~mascara].copy()
    logger.info(
        "[regras_negocio_bbagil] Remocao Ciclo 2: %d -> %d linhas",
        antes,
        len(df_filtrado),
    )
    return df_filtrado


def aplicar_limiar_bbagil(
    df_fato: pd.DataFrame, limiar: float = settings.LIMIAR_VALOR_BBAGIL
) -> pd.DataFrame:
    """Descarta, do ``fato_bbagil`` ja agregado por ente+beneficiario, os
    registros cujo valor total pago fica abaixo do limiar (ruido de
    transacoes residuais de valor muito baixo)."""
    if df_fato.empty:
        return df_fato
    antes = len(df_fato)
    df_filtrado = df_fato[df_fato["valor_transacao_total_bbagil"] >= limiar].copy()
    logger.info(
        "[regras_negocio_bbagil] Limiar R$%.2f: %d -> %d linhas",
        limiar,
        antes,
        len(df_filtrado),
    )
    return df_filtrado


def montar_fato_bbagil(
    df_extrato_filtrado: pd.DataFrame, df_subtransacoes_filtradas: pd.DataFrame
) -> pd.DataFrame:
    """Agrupa extrato (leaf, sem subtransacoes) + subtransacoes filtradas
    por (ente, beneficiario), somando o valor pago -- grao final do
    ``fato_bbagil``."""
    partes = []

    if not df_extrato_filtrado.empty:
        parte_extrato = df_extrato_filtrado.rename(
            columns={COL_VALOR_EXTRATO: "valor_pago"}
        )[[COL_ENTE, COL_BENEFICIARIO_DOC, "valor_pago"]]
        partes.append(parte_extrato)

    if not df_subtransacoes_filtradas.empty:
        parte_sub = df_subtransacoes_filtradas.rename(
            columns={COL_VALOR_SUBTRANSACAO: "valor_pago"}
        )[[COL_ENTE, COL_BENEFICIARIO_DOC, "valor_pago"]]
        partes.append(parte_sub)

    if not partes:
        return pd.DataFrame(
            columns=[
                "ente_bbagil",
                "documento_beneficiario_bbagil",
                "valor_transacao_total_bbagil",
            ]
        )

    df_uniao = pd.concat(partes, ignore_index=True)
    fato = (
        df_uniao.groupby([COL_ENTE, COL_BENEFICIARIO_DOC])["valor_pago"]
        .sum()
        .reset_index()
        .rename(
            columns={
                COL_ENTE: "ente_bbagil",
                COL_BENEFICIARIO_DOC: "documento_beneficiario_bbagil",
                "valor_pago": "valor_transacao_total_bbagil",
            }
        )
    )

    fato = aplicar_limiar_bbagil(fato)

    logger.info("[regras_negocio_bbagil] fato_bbagil final: %d linhas", len(fato))
    return fato
