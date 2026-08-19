"""Utilitários de composição semântica: mascaramento de dados sensíveis,
resolução de nomes de tabela do dicionário original para o nome no bronze,
e metadados de domínio por prefixo — usados pelo gerador de YAML e DOCX.
"""

from __future__ import annotations

import re
from typing import Any

DOMINIOS = {
    "sac": (
        "Núcleo do SALIC (Sistema de Apoio às Leis de Incentivo à Cultura) — "
        "projetos culturais incentivados, proponentes, captação de recursos, "
        "editais, prestação de contas, pareceres e patrocínio (Lei Rouanet e "
        "leis correlatas de incentivo fiscal à cultura)."
    ),
    "tabelas": (
        "Cadastro auxiliar do SALIC: pessoas, endereços, órgãos e domínios de "
        "apoio (categorias, localidades) usados pelo núcleo SAC."
    ),
    "agentes": (
        "Registro de identidade de agentes (pessoas físicas e jurídicas), "
        "compartilhado com outros domínios do MinC além do SALIC (ex.: LPG, PNAB)."
    ),
    "controledeacesso": (
        "Infraestrutura de controle de acesso aos sistemas do MinC — usuários, "
        "sistemas e permissões. Não é dado de negócio do SALIC."
    ),
    "bdcorporativo": (
        "Metadados técnicos do banco de dados corporativo (ex.: diagramas do "
        "SQL Server). Não é dado de negócio do SALIC."
    ),
}

SENSITIVE_COLUMN_RE = re.compile(
    r"cpf|cnpj|senha|password|e[-_]?mail|telefone|celular|\brg\b|\bpis\b|\bnis\b"
    r"|passaporte|cart[aã]o|titulo.*eleitor",
    re.IGNORECASE,
)

MAX_DISPLAY_LEN = 80


def is_sensitive_column(col_name: str) -> bool:
    return bool(SENSITIVE_COLUMN_RE.search(col_name))


def mask_value(value: Any) -> str:
    text = str(value)
    if len(text) <= 5:
        return "*" * len(text)
    return f"{text[:2]}{'*' * (len(text) - 4)}{text[-2:]}"


def display_value(col_name: str, value: Any) -> Any:
    """Aplica máscara em colunas sensíveis e trunca valores muito longos
    antes de irem para os artefatos finais (YAML/DOCX)."""
    if value is None:
        return None
    if is_sensitive_column(col_name):
        return mask_value(value)
    text = str(value)
    if len(text) > MAX_DISPLAY_LEN:
        return text[: MAX_DISPLAY_LEN - 1] + "…"
    return value


def build_origin_name_index(merged: dict[str, dict[str, Any]]) -> dict[str, str]:
    """nome_original_minusculo -> nome_no_bronze, para resolver os textos de
    relacionamento do dicionário original (que citam nomes sem prefixo/case
    original do SQL Server) de volta para as chaves usadas neste pipeline."""
    index: dict[str, str] = {}
    for bronze_name, entry in merged.items():
        origin = entry.get("nome_tabela_origem")
        if origin:
            index.setdefault(origin.lower(), bronze_name)
    return index
