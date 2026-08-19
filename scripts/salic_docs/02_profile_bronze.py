"""Perfila todas as tabelas do schema bronze (ou uma lista específica, via
--tables), gravando um checkpoint JSON por tabela em output/profile/. Reruns
pulam tabelas já perfiladas (idempotente) — use --force para reprocessar.

Uso:
    poetry run python scripts/salic_docs/02_profile_bronze.py
    poetry run python scripts/salic_docs/02_profile_bronze.py --tables sac__abrangencia,agentes__agentes
    poetry run python scripts/salic_docs/02_profile_bronze.py --limit 5
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.salic_docs.lib import db
from scripts.salic_docs.lib.profiler import profile_table

OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "profile"


def list_bronze_tables(conn) -> list[str]:
    cur = db.dict_cursor(conn)
    cur.execute(
        "select table_name from information_schema.tables "
        "where table_schema = 'bronze' order by table_name"
    )
    return [r["table_name"] for r in cur.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=str, default=None, help="lista separada por vírgula")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = db.get_connection()
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    else:
        tables = list_bronze_tables(conn)
        if args.limit:
            tables = tables[: args.limit]

    total = len(tables)
    done = 0
    skipped = 0
    failed: list[str] = []
    t0 = time.time()

    for i, table_name in enumerate(tables, start=1):
        out_path = OUTPUT_DIR / f"{table_name}.json"
        if out_path.exists() and not args.force:
            skipped += 1
            continue
        start = time.time()
        try:
            result = profile_table(conn, table_name)
        except Exception as exc:
            conn.rollback()
            failed.append(table_name)
            print(f"[{i}/{total}] FALHA {table_name}: {exc}", flush=True)
            continue
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        done += 1
        elapsed = time.time() - start
        print(
            f"[{i}/{total}] {table_name}: {result['total_rows']} linhas, "
            f"{len(result['columns'])} colunas, {elapsed:.1f}s",
            flush=True,
        )

    total_elapsed = time.time() - t0
    print(
        f"\nConcluído: {done} perfiladas, {skipped} já existentes (puladas), "
        f"{len(failed)} falharam, em {total_elapsed:.1f}s",
        flush=True,
    )
    if failed:
        print("Tabelas com falha:", failed, flush=True)


if __name__ == "__main__":
    main()
