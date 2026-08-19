"""Levantamento estatístico (perfil empírico) de uma tabela do schema
bronze: contagem de linhas, nulos, distintos, vazios, min/max e amostra de
valores reais por coluna. Todas as colunas no bronze são `text` (camada
bruta) — os tipos "de verdade" (int, datetime, bit...) vêm do dicionário
original (ver schemaspy_parser.py), não do Postgres.

Cuidado de performance: COUNT(DISTINCT col) exige hash/sort por coluna e é
de longe o passo mais caro (medido: ~45s para contagem exata de distintos em
9 colunas x ~1M linhas, contra ~2s para a passada simples de nulos/vazios/
min/max nas mesmas colunas). Por isso a contagem exata de distintos só roda
para tabelas pequenas (<= EXACT_DISTINCT_MAX_ROWS); acima disso usamos a
estimativa `n_distinct` do catálogo do Postgres (pg_stats, calculada pelo
autovacuum/ANALYZE) — aproximada, e marcada como tal no resultado.
"""

from __future__ import annotations

from typing import Any

from psycopg2 import sql

DOMAIN_ENUM_MAX_DISTINCT = 30
DOMAIN_ENUM_MAX_ROWS = 6_000_000  # GROUP BY em coluna de baixa cardinalidade é barato (custo ~ scan)
EXACT_DISTINCT_MAX_ROWS = 100_000  # acima disso, usa estimativa via pg_stats
NUMERIC_MINMAX_MAX_ROWS = 100_000  # regex por linha é caro; só compensa em tabelas pequenas
SAMPLE_ROWS = 5
NUMERIC_RE = r"^\s*-?\d+(\.\d+)?\s*$"


def _columns(cur: Any, table_name: str) -> list[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'bronze' and table_name = %s
        order by ordinal_position
        """,
        (table_name,),
    )
    return [r["column_name"] for r in cur.fetchall()]


def _build_base_query(table_name: str, columns: list[str], *, with_numeric: bool) -> sql.Composed:
    table_ident = sql.Identifier("bronze", table_name)
    parts = [sql.SQL("count(*) as total_rows")]
    for col in columns:
        col_ident = sql.Identifier(col)
        prefix = col.replace('"', "")
        parts.extend(
            [
                sql.SQL("count({0}) as {1}").format(
                    col_ident, sql.Identifier(f"{prefix}__nonnull")
                ),
                sql.SQL("count(*) filter (where {0} = '') as {1}").format(
                    col_ident, sql.Identifier(f"{prefix}__empty")
                ),
                sql.SQL("min({0}) as {1}").format(
                    col_ident, sql.Identifier(f"{prefix}__min")
                ),
                sql.SQL("max({0}) as {1}").format(
                    col_ident, sql.Identifier(f"{prefix}__max")
                ),
            ]
        )
        if with_numeric:
            parts.extend(
                [
                    sql.SQL(
                        "min(case when {0} ~ %s then {0}::numeric end) as {1}"
                    ).format(col_ident, sql.Identifier(f"{prefix}__num_min")),
                    sql.SQL(
                        "max(case when {0} ~ %s then {0}::numeric end) as {1}"
                    ).format(col_ident, sql.Identifier(f"{prefix}__num_max")),
                ]
            )
    query = sql.SQL("select {0} from {1}").format(sql.SQL(", ").join(parts), table_ident)
    return query


def _run_base(cur: Any, table_name: str, columns: list[str], *, with_numeric: bool) -> dict[str, Any]:
    query = _build_base_query(table_name, columns, with_numeric=with_numeric)
    params = [NUMERIC_RE] * (len(columns) * 2) if with_numeric else []
    cur.execute(query, params)
    return dict(cur.fetchone())


def _run_exact_distinct(cur: Any, table_name: str, columns: list[str]) -> dict[str, int]:
    table_ident = sql.Identifier("bronze", table_name)
    parts = [
        sql.SQL("count(distinct {0}) as {1}").format(
            sql.Identifier(col), sql.Identifier(f"{col.replace(chr(34), '')}__distinct")
        )
        for col in columns
    ]
    query = sql.SQL("select {0} from {1}").format(sql.SQL(", ").join(parts), table_ident)
    cur.execute(query)
    return dict(cur.fetchone())


def _estimate_distinct(cur: Any, table_name: str) -> dict[str, int]:
    cur.execute(
        "select attname, n_distinct from pg_stats where schemaname = 'bronze' and tablename = %s",
        (table_name,),
    )
    return {r["attname"]: r["n_distinct"] for r in cur.fetchall()}


def _sample_rows(cur: Any, table_name: str, columns: list[str]) -> list[dict[str, Any]]:
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    query = sql.SQL("select {0} from {1} limit {2}").format(
        col_idents, sql.Identifier("bronze", table_name), sql.Literal(SAMPLE_ROWS)
    )
    cur.execute(query)
    return [dict(r) for r in cur.fetchall()]


def _domain_enum(cur: Any, table_name: str, col: str) -> list[dict[str, Any]]:
    query = sql.SQL(
        "select {0} as value, count(*) as freq from {1} group by {0} order by freq desc limit %s"
    ).format(sql.Identifier(col), sql.Identifier("bronze", table_name))
    cur.execute(query, (DOMAIN_ENUM_MAX_DISTINCT,))
    return [dict(r) for r in cur.fetchall()]


def profile_table(conn: Any, table_name: str) -> dict[str, Any]:
    from scripts.salic_docs.lib.db import dict_cursor

    cur = dict_cursor(conn)
    columns = _columns(cur, table_name)
    if not columns:
        return {"table_name": table_name, "total_rows": 0, "columns": {}, "sample_rows": []}

    # 1) passada barata: total, nulos, vazios, min/max textual (sempre roda)
    #    + min/max numérico só se a tabela for pequena (regex por linha é caro).
    agg = _run_base(cur, table_name, columns, with_numeric=False)
    total_rows = agg["total_rows"]

    numeric_agg: dict[str, Any] = {}
    if 0 < total_rows <= NUMERIC_MINMAX_MAX_ROWS:
        numeric_agg = _run_base(cur, table_name, columns, with_numeric=True)

    # 2) distintos: exato (barato) para tabelas pequenas, estimado via pg_stats
    #    (catálogo, custo ~zero) para tabelas grandes.
    distinct_source = "estimado_pg_stats"
    exact_distinct: dict[str, int] = {}
    estimate: dict[str, float] = {}
    if 0 < total_rows <= EXACT_DISTINCT_MAX_ROWS:
        exact_distinct = _run_exact_distinct(cur, table_name, columns)
        distinct_source = "exato"
    else:
        estimate = _estimate_distinct(cur, table_name)

    col_stats: dict[str, Any] = {}
    for col in columns:
        prefix = col.replace('"', "")
        nonnull = agg[f"{prefix}__nonnull"]
        null_count = total_rows - nonnull

        if distinct_source == "exato":
            distinct_count = exact_distinct.get(f"{prefix}__distinct")
        else:
            n_dist = estimate.get(col)
            if n_dist is None:
                distinct_count = None
            elif n_dist < 0:
                distinct_count = round(abs(n_dist) * total_rows)
            else:
                distinct_count = round(n_dist)

        col_stats[col] = {
            "nonnull_count": nonnull,
            "null_count": null_count,
            "null_pct": round(null_count / total_rows * 100, 2) if total_rows else None,
            "distinct_count": distinct_count,
            "distinct_count_fonte": distinct_source if total_rows else None,
            "empty_count": agg[f"{prefix}__empty"],
            "min_text": agg[f"{prefix}__min"],
            "max_text": agg[f"{prefix}__max"],
            "numeric_min": numeric_agg.get(f"{prefix}__num_min"),
            "numeric_max": numeric_agg.get(f"{prefix}__num_max"),
        }

    if total_rows > 0:
        try:
            sample = _sample_rows(cur, table_name, columns)
        except Exception:
            sample = []
    else:
        sample = []

    if 0 < total_rows <= DOMAIN_ENUM_MAX_ROWS:
        for col in columns:
            stats = col_stats[col]
            dc = stats["distinct_count"]
            if dc is not None and 0 < dc <= DOMAIN_ENUM_MAX_DISTINCT:
                try:
                    stats["value_frequency"] = _domain_enum(cur, table_name, col)
                except Exception:
                    stats["value_frequency"] = None

    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "columns": col_stats,
        "sample_rows": sample,
    }
