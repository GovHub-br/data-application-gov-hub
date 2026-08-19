"""Faz parse do dicionário de dados original do SALIC (export SchemaSpy) e
grava o resultado estruturado em output/dictionary.json.

Uso:
    poetry run python scripts/salic_docs/01_parse_dictionary.py [caminho_do_dicionario]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.salic_docs.lib.schemaspy_parser import parse_dictionary

DEFAULT_DICT_PATH = Path(
    "/home/bottinolucas/Área de trabalho/Dados/SALIC/Dicionário de Dados Salic/Dicionário de Dados"
)
OUTPUT_PATH = Path(__file__).resolve().parent / "output" / "dictionary.json"


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DICT_PATH
    if not root.exists():
        raise SystemExit(f"Caminho não encontrado: {root}")

    result = parse_dictionary(root)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2))

    with_desc = sum(1 for v in result.values() if v["description"])
    total_cols = sum(len(v["columns"]) for v in result.values())
    cols_with_comment = sum(1 for v in result.values() for c in v["columns"] if c["comment"])
    print(f"Tabelas parseadas: {len(result)}")
    print(f"Com descrição: {with_desc} ({with_desc / len(result):.0%})")
    print(f"Colunas: {total_cols}, com comentário: {cols_with_comment} ({cols_with_comment / total_cols:.1%})")
    print(f"Salvo em {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
