"""Junta output/dictionary.json (dicionário original) com output/profile/*.json
(perfil observado no bronze) em um único output/merged.json, uma entrada por
tabela do bronze, com a heurística de sinalização de tabelas suspeitas
aplicada (campo `observacao`).

Uso:
    poetry run python scripts/salic_docs/03_flag_and_merge.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.salic_docs.lib import db
from scripts.salic_docs.lib.merge import merge_table

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DICTIONARY_PATH = OUTPUT_DIR / "dictionary.json"
PROFILE_DIR = OUTPUT_DIR / "profile"
MERGED_PATH = OUTPUT_DIR / "merged.json"


def main() -> None:
    dictionary = json.loads(DICTIONARY_PATH.read_text()) if DICTIONARY_PATH.exists() else {}

    conn = db.get_connection()
    cur = db.dict_cursor(conn)
    cur.execute(
        "select table_name from information_schema.tables "
        "where table_schema = 'bronze' order by table_name"
    )
    bronze_tables = [r["table_name"] for r in cur.fetchall()]
    all_names = set(bronze_tables)

    merged = {}
    missing_profile = []
    for table_name in bronze_tables:
        prefix = table_name.split("__", 1)[0] if "__" in table_name else ""
        dict_entry = dictionary.get(table_name)
        profile_path = PROFILE_DIR / f"{table_name}.json"
        profile_entry = None
        if profile_path.exists():
            profile_entry = json.loads(profile_path.read_text())
        else:
            missing_profile.append(table_name)
        merged[table_name] = merge_table(
            table_name, prefix, dict_entry, profile_entry, all_bronze_table_names=all_names
        )

    MERGED_PATH.write_text(json.dumps(merged, ensure_ascii=False, indent=2, default=str))

    flagged = sum(1 for m in merged.values() if m["observacao"])
    documented = sum(1 for m in merged.values() if m["presente_no_dicionario_original"])
    print(f"Tabelas mescladas: {len(merged)}")
    print(f"Com entrada no dicionário original: {documented}")
    print(f"Sinalizadas (observacao não vazia): {flagged}")
    if missing_profile:
        print(f"Sem perfil ainda ({len(missing_profile)}): rode 02_profile_bronze.py primeiro")
    print(f"Salvo em {MERGED_PATH}")


if __name__ == "__main__":
    main()
