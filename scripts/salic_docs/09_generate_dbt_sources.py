"""Gera as declarações dbt `sources:` (formato dbt 1.12 + meta OpenMetadata)
para o schema bronze do SALIC, a partir de output/merged.json — um arquivo
por schema de origem (sac/tabelas/agentes/controledeacesso/bdcorporativo),
em dbt/minc/models/salic_bronze/, para o conector dbt do OpenMetadata
reconhecer descrições, tags e tier dessas tabelas.

Cada schema vira um `source` dbt com nome próprio (bronze_sac, bronze_tabelas,
...) porque o dbt não permite dividir um mesmo source entre vários arquivos
— mas todos compartilham `schema: bronze` (o schema físico real no Postgres
é um só). Isso espelha a divisão por schema já feita no YAML semântico.

Uso:
    poetry run python scripts/salic_docs/09_generate_dbt_sources.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.salic_docs.lib.dbt_source_builder import build_table_entry
from scripts.salic_docs.lib.dbt_yaml import dump_dbt_yaml
from scripts.salic_docs.lib.semantics import DOMINIOS

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
MERGED_PATH = OUTPUT_DIR / "merged.json"
DBT_MODELS_DIR = Path(__file__).resolve().parents[2] / "dbt" / "minc" / "models"
TARGET_DIR = DBT_MODELS_DIR / "salic_bronze"

SCHEMA_ORDER = ["sac", "tabelas", "agentes", "controledeacesso", "bdcorporativo"]

# Camada bronze = dado bruto, não tratado — tier baixo e uniforme por
# decisão deliberada (não diferenciamos por tabela: a flag de "possível
# obsoleta" já cobre isso via tag `possivel_obsoleta`, não via tier).
TIER = "Tier.Tier4"
DOMAIN_VALUE = "Cultura"


def main() -> None:
    if not MERGED_PATH.exists():
        raise SystemExit("output/merged.json não encontrado — rode 03_flag_and_merge.py antes.")

    merged: dict[str, dict[str, Any]] = json.loads(MERGED_PATH.read_text())

    by_prefix: dict[str, list[dict[str, Any]]] = {}
    for entry in merged.values():
        by_prefix.setdefault(entry["prefixo"], []).append(entry)

    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    total_tests = 0

    for prefix in SCHEMA_ORDER:
        entries = by_prefix.get(prefix)
        if not entries:
            continue
        entries = sorted(entries, key=lambda e: e["nome_tabela"])

        tables = [build_table_entry(e, tier=TIER, domain_value=DOMAIN_VALUE) for e in entries]
        for t in tables:
            for c in t["columns"]:
                total_tests += len(c.get("tests") or [])

        doc = {
            "version": 2,
            "sources": [
                {
                    "name": f"bronze_{prefix}",
                    "schema": "bronze",
                    "description": (
                        f"{DOMINIOS.get(prefix, '')} Camada bronze (dados brutos do SALIC, "
                        "réplica do sistema de origem via Airflow, sem tratamento). Descrições "
                        "e tiers gerados automaticamente a partir do dicionário de dados "
                        "original do SALIC (export SchemaSpy) cruzado com o levantamento "
                        "estatístico do bronze — ver dbt/minc/docs/salic/ para o catálogo "
                        "completo (YAML semântico, dicionário e catálogo em DOCX/HTML)."
                    ),
                    "tables": tables,
                }
            ],
        }

        out_path = TARGET_DIR / f"sources_{prefix}.yml"
        out_path.write_text(dump_dbt_yaml(doc), encoding="utf-8")
        print(f"  {out_path.relative_to(DBT_MODELS_DIR.parents[1])}: {len(tables)} tabelas")

    print(f"\nTotal de testes dbt gerados (evidência forte apenas): {total_tests}")


if __name__ == "__main__":
    main()
