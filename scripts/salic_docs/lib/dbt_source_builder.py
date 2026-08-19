"""Constrói entradas dbt `sources:` (schema.yml, formato dbt 1.12 + meta
OpenMetadata) a partir de output/merged.json — uma entrada por tabela do
bronze, com testes dbt SÓ onde a evidência é forte:

  - not_null: coluna com PK documentada no dicionário original E 0 nulos
    observados nos dados hoje (evidência documentada + observada concordam).
  - unique: idem, mas só quando a contagem de distintos é EXATA (tabela
    pequena o suficiente pra não depender de estimativa do pg_stats) e bate
    exatamente com a contagem de não-nulos.
  - accepted_values: só quando um check constraint original do SQL Server
    enumera o domínio de uma única coluna via `[col]='v1' OR [col]='v2' ...`,
    E (quando temos amostra) os valores observados são um subconjunto do que
    o constraint permite — senão o teste nasceria quebrado.

Nada é inventado: sem essas evidências, a coluna não recebe teste.
"""

from __future__ import annotations

import re
from typing import Any

CHECK_DOMAIN_RE = re.compile(r"\[(\w+)\]\s*=\s*'([^']*)'")

EXACT_DISTINCT_MAX_ROWS_NOTE = "exato"


def _parse_check_constraint_domain(expression: str) -> tuple[str, list[str]] | None:
    """Extrai (coluna, valores) de expressões tipo
    "([siAbrangencia]='0' OR [siAbrangencia]='1' OR [siAbrangencia]='2')".
    Retorna None se a expressão referenciar mais de uma coluna, ou não casar
    o padrão de igualdade simples."""
    matches = CHECK_DOMAIN_RE.findall(expression)
    if not matches:
        return None
    columns = {m[0] for m in matches}
    if len(columns) != 1:
        return None
    col = next(iter(columns))
    values = [m[1] for m in matches]
    # dedup preservando ordem
    seen: set[str] = set()
    deduped = []
    for v in values:
        if v not in seen:
            seen.add(v)
            deduped.append(v)
    return col, deduped


def _column_domains(entry: dict[str, Any]) -> dict[str, list[str]]:
    """Mapa nome_coluna_minusculo -> valores aceitos, coletado de todos os
    check constraints documentados da tabela."""
    domains: dict[str, list[str]] = {}
    for c in entry.get("check_constraints_documentados") or []:
        parsed = _parse_check_constraint_domain(c["expression"])
        if parsed is None:
            continue
        col, values = parsed
        domains[col.lower()] = values
    return domains


def _build_column_tests(col: dict[str, Any], accepted_values: list[str] | None) -> list[Any]:
    tests: list[Any] = []

    is_pk = bool(col.get("e_chave_primaria_documentada"))
    null_count = col.get("null_count")
    nonnull_count = col.get("nonnull_count")
    distinct_count = col.get("distinct_count")
    distinct_fonte = col.get("distinct_count_fonte")

    if is_pk and null_count == 0:
        tests.append("not_null")
        if (
            distinct_fonte == EXACT_DISTINCT_MAX_ROWS_NOTE
            and nonnull_count
            and distinct_count == nonnull_count
        ):
            tests.append("unique")

    if accepted_values:
        observed = col.get("value_frequency")
        if observed:
            observed_values = {str(v["value"]) for v in observed}
            accepted_set = set(accepted_values)
            if not observed_values.issubset(accepted_set):
                accepted_values = None  # evidência observada contradiz o constraint — não arrisca
        if accepted_values:
            tests.append({"accepted_values": {"arguments": {"values": accepted_values}}})

    return tests


def _column_description(col: dict[str, Any]) -> str:
    parts = []
    if col.get("comentario_original"):
        parts.append(col["comentario_original"])
    else:
        parts.append("[não documentado no dicionário de dados original do SALIC]")
    tipo = col.get("tipo_documentado")
    if tipo:
        tam = col.get("tamanho_documentado")
        tipo_str = f"{tipo}" + (f"({tam})" if tam else "")
        parts.append(f"Tipo original (SQL Server): {tipo_str}.")
    return " ".join(parts)


def build_column_entry(col: dict[str, Any], domains: dict[str, list[str]]) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": col["name"],
        "description": _column_description(col),
        "data_type": "text",  # tipo real no bronze — todas as colunas chegam cruas como text
    }
    accepted_values = domains.get(col["name"].lower())
    tests = _build_column_tests(col, accepted_values)
    if tests:
        entry["tests"] = tests
    return entry


def build_table_entry(table_entry: dict[str, Any], *, tier: str, domain_value: str) -> dict[str, Any]:
    domains = _column_domains(table_entry)

    tags = ["bronze", "salic", table_entry["prefixo"]]
    if table_entry.get("observacao"):
        tags.append("possivel_obsoleta")
    if not table_entry.get("presente_no_dicionario_original"):
        tags.append("nao_documentado")

    description = table_entry.get("descricao") or (
        "[Não documentado no dicionário de dados original do SALIC — tabela presente "
        "apenas no bronze, sem descrição confirmada.]"
    )
    if table_entry.get("observacao"):
        obs_joined = " ".join(table_entry["observacao"])
        description = f"{description}\n\nObservações do levantamento automatizado: {obs_joined}"

    columns = [build_column_entry(c, domains) for c in table_entry["colunas"] if not c.get("ausente_do_perfil_atual")]

    return {
        "name": table_entry["nome_tabela"],
        "description": description,
        "config": {"tags": tags},
        "meta": {
            "openmetadata": {
                "tier": tier,
                "domain": domain_value,
                # Certification é mutuamente exclusiva (Bronze/Silver/Gold) e
                # bate literalmente com a camada medalhão desses dados —
                # requer a Classification "Certification" já cadastrada no OM.
                "tags": ["Certification.Bronze"],
            }
        },
        "columns": columns,
    }
