"""Construtores de payload para os endpoints do BSC/SERPRO.

Todos os payloads carregam os campos de auditoria obrigatorios
(``cacheEvict``, ``cpfConsulta``, ``ipOrigem``, ``ipUsuario``, ``usuario``).

A validacao de CPF (11 digitos) e CNPJ (14 digitos) e feita aqui, na
fronteira de montagem do payload, e nao pode ser contornada por quem chama
-- e a correcao definitiva do bug do legado (``cpf.py:65``) onde o filtro de
tamanho ficou comentado e CPFs invalidos eram enviados a API.
"""

import re
from typing import Any

import config_bsc_pnab as settings


def normalize_cpf(valor: str) -> str:
    """Remove formatacao e valida que o CPF tem exatamente 11 digitos."""
    digitos = re.sub(r"\D", "", str(valor))
    if len(digitos) != 11:
        raise ValueError(f"CPF invalido (esperado 11 digitos): {valor!r}")
    return digitos


def normalize_cnpj(valor: str) -> str:
    """Remove formatacao e valida que o CNPJ tem exatamente 14 digitos."""
    digitos = re.sub(r"\D", "", str(valor))
    if len(digitos) != 14:
        raise ValueError(f"CNPJ invalido (esperado 14 digitos): {valor!r}")
    return digitos


def _base_auditoria() -> dict[str, Any]:
    return {
        "cacheEvict": False,
        "cpfConsulta": settings.BSC_CPF_CONSULTA,
        "ipOrigem": settings.BSC_IP_ORIGEM,
        "ipUsuario": settings.BSC_IP_USUARIO,
        "usuario": settings.BSC_USUARIO,
    }


def build_payload_cpf_list(cpfs: list[str]) -> dict[str, Any]:
    if len(cpfs) > 45:
        raise ValueError(f"Maximo de 45 CPFs por lote, recebido {len(cpfs)}")
    return {
        **_base_auditoria(),
        "cpfs": [normalize_cpf(cpf) for cpf in cpfs],
    }


def build_payload_cnpj_detalhe(cnpj: str) -> dict[str, Any]:
    return {**_base_auditoria(), "cnpj": normalize_cnpj(cnpj)}


def build_payload_cnpj_basico(cnpj: str) -> dict[str, Any]:
    return build_payload_cnpj_detalhe(cnpj)


def build_payload_cadunico(cpf_beneficiario: str) -> dict[str, Any]:
    return {
        **_base_auditoria(),
        "authorizationId": settings.BSC_CADUNICO_AUTHORIZATION_ID,
        "authorizationIdType": settings.BSC_CADUNICO_AUTHORIZATION_ID_TYPE,
        "consumerId": settings.BSC_CADUNICO_CONSUMER_ID,
        "consumerIdType": settings.BSC_CADUNICO_CONSUMER_ID_TYPE,
        "dadoConsulta": normalize_cpf(cpf_beneficiario),
        "subjectId": settings.BSC_CPF_CONSULTA,
        "subjectIdType": "CPF",
    }


def build_payload_bpc(cpf: str) -> dict[str, Any]:
    return {**_base_auditoria(), "cpf": normalize_cpf(cpf)}


def build_payload_relacao_trabalhista(cpf: str) -> dict[str, Any]:
    return {**_base_auditoria(), "cpf": normalize_cpf(cpf)}


def build_payload_bbagil_extrato_controle(
    agencia: int, numero_conta: int, periodo_inicial: str, periodo_final: str
) -> dict[str, Any]:
    """``periodo_inicial``/``periodo_final`` no formato YYYY-MM-DD."""
    return {
        **_base_auditoria(),
        "agencia": agencia,
        "numeroConta": numero_conta,
        "periodoInicial": periodo_inicial,
        "periodoFinal": periodo_final,
    }


def build_payload_bbagil_saldo_conta(agencia: int, numero_conta: int) -> dict[str, Any]:
    """Usado pelos endpoints de saldo (conta corrente e aplicacoes
    financeiras), que compartilham o mesmo contrato de requisicao do
    extrato menos os campos de periodo."""
    return {
        **_base_auditoria(),
        "agencia": agencia,
        "numeroConta": numero_conta,
    }


def build_payload_bbagil_subtransacoes(
    agencia: str, numero_conta: str, id_transaction: str
) -> dict[str, Any]:
    """Ao contrario do extrato, agencia/numeroConta sao string neste
    endpoint (confirmado no contrato do BSC)."""
    return {
        **_base_auditoria(),
        "agencia": str(agencia),
        "numeroConta": str(numero_conta),
        "id": str(id_transaction),
    }
