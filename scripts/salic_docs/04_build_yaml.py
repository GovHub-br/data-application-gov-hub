"""Gera o YAML semântico final (salic_semantic_layer.yaml) a partir de
output/merged.json, pensado como base de conhecimento para RAG: descrições
de tabela/coluna, domínio de negócio, relacionamentos, códigos e
significados, e diferenciação clara entre o que é documentado na origem,
observado nos dados hoje, inferido por este pipeline, ou não documentado.

Uso:
    poetry run python scripts/salic_docs/04_build_yaml.py
"""

from __future__ import annotations

import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.salic_docs.lib.semantics import (
    DOMINIOS,
    build_origin_name_index,
    display_value,
    is_sensitive_column,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
MERGED_PATH = OUTPUT_DIR / "merged.json"
FINAL_DIR = Path(__file__).resolve().parents[2] / "dbt" / "minc" / "docs" / "salic"
SCHEMAS_DIR = FINAL_DIR / "schemas"
INDEX_YAML_PATH = FINAL_DIR / "salic_semantic_layer.yaml"

SCHEMA_ORDER = ["sac", "tabelas", "agentes", "controledeacesso", "bdcorporativo"]

REL_RE = re.compile(r"references\s+(\S+)\.(\S+)\s+via\s+(\S+)", re.IGNORECASE)


def _parse_relationship(sentence: str, origin_index: dict[str, str]) -> dict[str, Any]:
    m = REL_RE.search(sentence)
    if not m:
        return {"descricao_origem": sentence}
    target_table, target_col, constraint = m.groups()
    resolved = origin_index.get(target_table.lower())
    return {
        "tabela_referenciada_origem": target_table,
        "tabela_referenciada_bronze": resolved,
        "coluna_referenciada": target_col,
        "constraint": constraint,
        "descricao_origem": sentence,
    }


def _build_column(col: dict[str, Any], origin_index: dict[str, str]) -> dict[str, Any]:
    sensitive = is_sensitive_column(col["name"])
    out: dict[str, Any] = {
        "nome": col["name"],
        "tipo_documentado": col.get("tipo_documentado"),
        "tamanho_documentado": col.get("tamanho_documentado"),
        "permite_null_documentado": col.get("permite_null_documentado"),
        "chave_primaria_documentada": bool(col.get("e_chave_primaria_documentada")),
        "chave_estrangeira_documentada": bool(col.get("e_chave_estrangeira_documentada")),
        "significado": col.get("comentario_original"),
        "significado_fonte": "documentado" if col.get("comentario_original") else "nao_documentado",
        "dado_sensivel": sensitive,
    }

    refs = col.get("referencias_documentadas") or []
    if refs:
        out["referencia_para"] = [_parse_relationship(r, origin_index) for r in refs]
    child_refs = col.get("referenciado_por_documentado") or []
    if child_refs:
        out["referenciado_por"] = child_refs

    if col.get("ausente_do_perfil_atual"):
        out["observacao_coluna"] = (
            "[pipeline] coluna presente no dicionário original, mas não encontrada "
            "no perfil atual do bronze (pode ter sido removida da ingestão)."
        )
        return out

    null_count = col.get("null_count")
    null_pct = col.get("null_pct")
    distinct_count = col.get("distinct_count")
    out.update(
        {
            "nulos": {"quantidade": null_count, "percentual": null_pct},
            "vazios": {"quantidade": col.get("empty_count")},
            "distintos": {
                "quantidade": distinct_count,
                "fonte": col.get("distinct_count_fonte"),
            },
            "min_observado": display_value(col["name"], col.get("min_text")),
            "max_observado": display_value(col["name"], col.get("max_text")),
        }
    )
    if col.get("numeric_min") is not None or col.get("numeric_max") is not None:
        out["min_numerico_observado"] = col.get("numeric_min")
        out["max_numerico_observado"] = col.get("numeric_max")

    value_freq = col.get("value_frequency")
    if value_freq:
        out["valores_observados"] = [
            {"valor": display_value(col["name"], v["value"]), "frequencia": v["freq"]}
            for v in value_freq
        ]
        out["valores_observados_fonte"] = "observado (enumeração completa se distintos <= 30, tabela <= 6M linhas)"

    total_nonnull = col.get("nonnull_count") or 0
    is_key_like = bool(
        col.get("e_chave_primaria_documentada")
        or (distinct_count is not None and total_nonnull and distinct_count >= total_nonnull * 0.95 and total_nonnull > 10)
    )
    out["aparenta_ser_identificador"] = is_key_like

    if col.get("nao_documentada"):
        out["observacao_coluna"] = (
            "[nao_documentado] coluna ausente do dicionário de dados original do "
            "SALIC — significado não confirmado; ver nome, tipo observado e "
            "exemplos como única evidência disponível."
        )

    return out


def _build_table(entry: dict[str, Any], origin_index: dict[str, str]) -> dict[str, Any]:
    columns = [_build_column(c, origin_index) for c in entry["colunas"]]
    sample = []
    for row in entry.get("amostra_linhas", [])[:5]:
        sample.append({k: display_value(k, v) for k, v in row.items()})

    return {
        "schema": entry["schema"],
        "nome_tabela": entry["nome_tabela"],
        "nome_origem": entry.get("nome_tabela_origem"),
        "dominio": entry["prefixo"],
        "descricao": entry.get("descricao"),
        "descricao_fonte": entry["descricao_fonte"],
        "presente_no_dicionario_original": entry["presente_no_dicionario_original"],
        "presente_no_perfil_atual": entry.get("presente_no_perfil_atual", True),
        "linhas_atuais_bronze": entry["linhas_atuais_bronze"],
        "linhas_snapshot_dicionario_original": entry.get("linhas_snapshot_dicionario_original"),
        "quantidade_colunas": entry["quantidade_colunas"],
        "chave_primaria_documentada": entry.get("chave_primaria_documentada") or [],
        "indices_documentados": [
            {"nome": i["name"], "tipo": i["type"], "colunas": i["columns"]}
            for i in entry.get("indices_documentados") or []
        ],
        "regras_de_negocio_documentadas": [
            {"nome": c["name"], "expressao": c["expression"]}
            for c in entry.get("check_constraints_documentados") or []
        ],
        "observacao": entry.get("observacao") or [],
        "amostra_linhas_observada": sample,
        "colunas": columns,
    }


def main() -> None:
    if not MERGED_PATH.exists():
        raise SystemExit("output/merged.json não encontrado — rode 03_flag_and_merge.py antes.")

    merged: dict[str, dict[str, Any]] = json.loads(MERGED_PATH.read_text())
    origin_index = build_origin_name_index(merged)

    tabelas = {
        name: _build_table(entry, origin_index) for name, entry in sorted(merged.items())
    }

    by_schema: dict[str, dict[str, Any]] = {}
    for name, table in tabelas.items():
        by_schema.setdefault(table["dominio"], {})[name] = table

    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    schema_files: dict[str, str] = {}
    for schema in SCHEMA_ORDER:
        schema_tabelas = by_schema.get(schema, {})
        rel_path = f"schemas/{schema}.yaml"
        schema_files[schema] = rel_path
        schema_doc = {
            "schema": schema,
            "descricao": DOMINIOS.get(schema, ""),
            "total_tabelas": len(schema_tabelas),
            "tabelas": schema_tabelas,
        }
        with (FINAL_DIR / rel_path).open("w", encoding="utf-8") as f:
            yaml.dump(schema_doc, f, allow_unicode=True, sort_keys=False, width=100)
        print(f"  {rel_path}: {len(schema_tabelas)} tabelas")

    doc = {
        "metadata": {
            "titulo": "Camada semântica — SALIC (schema bronze) — índice",
            "gerado_em": dt.datetime.now().isoformat(timespec="seconds"),
            "fonte_dados": "schema bronze do data warehouse do MinC (dados brutos, não tratados)",
            "fonte_documentacao_original": (
                "Dicionário de dados do SALIC — export SchemaSpy 7.0.2 do banco "
                "SQL Server de origem (snapshot dez/2025), fornecido pela equipe."
            ),
            "aviso_camada_bronze": (
                "Estes dados são a camada bruta (bronze): réplica do sistema de "
                "origem sem tratamento, deduplicação ou padronização. Podem "
                "conter duplicidades, valores inconsistentes e tabelas técnicas "
                "de backup/teste/versão — sinalizadas em `observacao` quando "
                "identificadas por heurística de nome, mas não removidas."
            ),
            "legenda_proveniencia": {
                "documentado": "Vem do dicionário de dados original do SALIC (SchemaSpy).",
                "observado": "Vem do levantamento estatístico feito no bronze na data de geração deste arquivo.",
                "inferido": "Dedução deste pipeline a partir de nome/padrão — sinalizada como tal, não é fato confirmado.",
                "nao_documentado": "Não foi possível determinar com segurança; não deve ser tratado como fato.",
                "heuristica": "Regra automática de sinalização (ex.: nome sugere backup) — indício, não confirmação.",
            },
            "total_tabelas": len(tabelas),
            "nota_estrutura": (
                "As tabelas ficam em arquivos separados por schema (ver `schemas` "
                "abaixo), no mesmo padrão schema__tabela usado no bronze — cada "
                "arquivo cobre um schema de origem, para facilitar edição e diff."
            ),
        },
        "dominios": DOMINIOS,
        "schemas": {
            schema: {
                "arquivo": rel_path,
                "descricao": DOMINIOS.get(schema, ""),
                "total_tabelas": len(by_schema.get(schema, {})),
            }
            for schema, rel_path in schema_files.items()
        },
    }

    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    with INDEX_YAML_PATH.open("w", encoding="utf-8") as f:
        yaml.dump(doc, f, allow_unicode=True, sort_keys=False, width=100)

    print(f"Índice gerado em {INDEX_YAML_PATH} ({len(tabelas)} tabelas em {len(schema_files)} arquivos de schema)")


if __name__ == "__main__":
    main()
