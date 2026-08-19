"""Junta o dicionário de dados original (SchemaSpy) com o perfil empírico
observado no bronze hoje, em um registro único por tabela. Aplica a
heurística de sinalização de tabelas suspeitas de serem backup/lixo/técnicas
(--> campo `observacao`), sem excluir nada do levantamento.

Convenção mantida em todo o pipeline: cada campo do resultado indica sua
proveniência —
  - "documentado": veio do dicionário de dados original do SALIC (SchemaSpy).
  - "observado": veio do levantamento estatístico feito hoje no bronze.
  - "inferido": dedução feita neste pipeline a partir de nome/padrão, marcada
    como tal — nunca apresentada como fato.
  - "nao_documentado": não foi possível determinar com segurança.
"""

from __future__ import annotations

import re
from typing import Any

DATE_PATTERNS = [
    re.compile(r"\d{4}_\d{2}_\d{2}"),
    re.compile(r"\d{8}(?!\d)"),
    re.compile(r"(19|20)\d{2}(?!\d)"),
]
JUNK_TERMS = [
    "bkp", "backup", "old", "antigo", "copia", "teste", "temp", "errata",
    "arquivado", "novo", "_v1", "_v2", "_ant",
]
ETL_LOG_TERMS = ["logcarga", "logerro", "log_carga", "log_erro"]
TRAILING_NUM_RE = re.compile(r"^(?P<base>.+?)(?P<suffix>\d{2,})$")


def _bare_name(table_name: str) -> str:
    """Nome sem o prefixo de schema, ex.: sac__abrangencia -> abrangencia."""
    return table_name.split("__", 1)[1] if "__" in table_name else table_name


def flag_table(
    table_name: str,
    *,
    in_dictionary: bool,
    row_count: int,
    is_profiled: bool,
    all_bronze_table_names: set[str],
) -> list[str]:
    """Heurísticas de "possível tabela obsoleta/backup/técnica". Cada item é
    um motivo explícito — não uma afirmação de que a tabela é de fato lixo."""
    reasons: list[str] = []
    bare = _bare_name(table_name)
    lower = bare.lower()

    if any(p.search(bare) for p in DATE_PATTERNS):
        reasons.append(
            "[heurística] nome contém um padrão de data — possível cópia/snapshot datado."
        )

    if any(term in lower for term in JUNK_TERMS):
        hit = next(term for term in JUNK_TERMS if term in lower)
        reasons.append(
            f"[heurística] nome contém o termo '{hit}', indicativo de backup/versão/teste."
        )

    if any(term in lower for term in ETL_LOG_TERMS):
        reasons.append(
            "[heurística] nome sugere tabela técnica de log de carga/erro de "
            "integração (ETL), não dado de negócio do SALIC."
        )

    m = TRAILING_NUM_RE.match(bare)
    if m and m.group("base") != bare:
        prefix = table_name.split("__", 1)[0] if "__" in table_name else ""
        base_candidate = f"{prefix}__{m.group('base')}" if prefix else m.group("base")
        if base_candidate in all_bronze_table_names and base_candidate != table_name:
            reasons.append(
                f"[heurística] nome termina em sufixo numérico e existe uma tabela "
                f"base '{base_candidate}' sem sufixo — possível variante/versão numerada."
            )

    if is_profiled and row_count == 0:
        reasons.append("[observado] tabela sem nenhuma linha no bronze hoje (0 registros).")

    if not in_dictionary:
        reasons.append(
            "[observado] tabela ausente do dicionário de dados original do SALIC "
            "(export SchemaSpy) — pode ter sido criada após a extração do "
            "dicionário, ou ser uma tabela técnica não catalogada na origem."
        )

    return reasons


def _match_dict_column(dict_columns: list[dict[str, Any]], col_name: str) -> dict[str, Any] | None:
    lower = col_name.lower()
    for c in dict_columns:
        if c["name"].lower() == lower:
            return c
    return None


def merge_table(
    table_name: str,
    prefix: str,
    dict_entry: dict[str, Any] | None,
    profile_entry: dict[str, Any] | None,
    *,
    all_bronze_table_names: set[str],
) -> dict[str, Any]:
    is_profiled = profile_entry is not None
    profile_entry = profile_entry or {"total_rows": None, "columns": {}, "sample_rows": []}
    dict_columns = (dict_entry or {}).get("columns", [])
    row_count = profile_entry.get("total_rows") or 0

    merged_columns = []
    seen_dict_names = set()
    for col_name, col_profile in profile_entry.get("columns", {}).items():
        dcol = _match_dict_column(dict_columns, col_name)
        if dcol:
            seen_dict_names.add(dcol["name"].lower())
        merged_columns.append(
            {
                "name": col_name,
                "tipo_documentado": dcol["type"] if dcol else None,
                "tamanho_documentado": dcol["size"] if dcol else None,
                "permite_null_documentado": dcol["nullable"] if dcol else None,
                "default_documentado": dcol["default"] if dcol else None,
                "e_chave_primaria_documentada": dcol["is_primary_key"] if dcol else None,
                "e_chave_estrangeira_documentada": dcol["is_foreign_key"] if dcol else None,
                "referencias_documentadas": dcol["parent_refs"] if dcol else [],
                "referenciado_por_documentado": dcol["children_refs"] if dcol else [],
                "comentario_original": dcol["comment"] if dcol else None,
                "nao_documentada": dcol is None,
                **col_profile,
            }
        )

    # Colunas que só existem no dicionário original (não vieram no perfil,
    # ex.: coluna removida do bronze ou tabela ainda não perfilada).
    for dcol in dict_columns:
        if dcol["name"].lower() not in seen_dict_names:
            merged_columns.append(
                {
                    "name": dcol["name"],
                    "tipo_documentado": dcol["type"],
                    "tamanho_documentado": dcol["size"],
                    "permite_null_documentado": dcol["nullable"],
                    "default_documentado": dcol["default"],
                    "e_chave_primaria_documentada": dcol["is_primary_key"],
                    "e_chave_estrangeira_documentada": dcol["is_foreign_key"],
                    "referencias_documentadas": dcol["parent_refs"],
                    "referenciado_por_documentado": dcol["children_refs"],
                    "comentario_original": dcol["comment"],
                    "nao_documentada": False,
                    "ausente_do_perfil_atual": True,
                }
            )

    observacoes = flag_table(
        table_name,
        in_dictionary=dict_entry is not None,
        row_count=row_count,
        is_profiled=is_profiled,
        all_bronze_table_names=all_bronze_table_names,
    )
    if not is_profiled:
        observacoes.append(
            "[pipeline] tabela ainda não perfilada nesta rodada — rode 02_profile_bronze.py."
        )

    return {
        "schema": "bronze",
        "prefixo": prefix,
        "nome_tabela": table_name,
        "nome_tabela_origem": (dict_entry or {}).get("table_name"),
        "descricao": (dict_entry or {}).get("description"),
        "descricao_fonte": "documentado" if (dict_entry or {}).get("description") else "nao_documentado",
        "presente_no_perfil_atual": is_profiled,
        "linhas_atuais_bronze": row_count,
        "linhas_snapshot_dicionario_original": (dict_entry or {}).get("row_count_snapshot"),
        "quantidade_colunas": len(merged_columns),
        "chave_primaria_documentada": (dict_entry or {}).get("primary_key") or [],
        "indices_documentados": (dict_entry or {}).get("indexes") or [],
        "check_constraints_documentados": (dict_entry or {}).get("check_constraints") or [],
        "presente_no_dicionario_original": dict_entry is not None,
        "observacao": observacoes,
        "amostra_linhas": profile_entry.get("sample_rows", []),
        "colunas": merged_columns,
    }
