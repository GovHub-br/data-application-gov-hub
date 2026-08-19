"""Gera o Dicionário de Dados em HTML (identidade visual GovHub real — CSS,
gradientes, sombras, cards) a partir de output/merged.json. Espelha o
conteúdo do DOCX técnico (05_generate_dicionario_dados.py), mas em HTML/CSS
puro, mais fiel ao design system do GovHub do que o DOCX consegue chegar.

Uso:
    poetry run python scripts/salic_docs/07_generate_html_dicionario.py [--tables t1,t2] [--out caminho.html]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.salic_docs.lib.html_common import (
    esc,
    page_shell,
    render_badge,
    render_bullet_list,
    render_cover,
    render_kv_table,
)
from scripts.salic_docs.lib.semantics import DOMINIOS, display_value

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
MERGED_PATH = OUTPUT_DIR / "merged.json"
FINAL_DIR = Path(__file__).resolve().parents[2] / "dbt" / "minc" / "docs" / "salic"
DEFAULT_OUT = FINAL_DIR / "dicionario_dados_salic.html"

SCHEMA_ORDER = ["sac", "tabelas", "agentes", "controledeacesso", "bdcorporativo"]
MAX_REL_SHOWN = 12


def _fmt_num(value: Any) -> str:
    if value is None:
        return "&mdash;"
    try:
        return f"{int(value):,}".replace(",", ".")
    except (TypeError, ValueError):
        return esc(value)


def _fmt_pct(value: Any) -> str:
    return f"{value:.2f}%" if value is not None else "&mdash;"


def _origem_dados(entry: dict[str, Any]) -> str:
    if entry["presente_no_dicionario_original"]:
        return (
            f"Sistema SALIC (SQL Server) &mdash; schema de origem "
            f"'{esc(entry['prefixo'])}', confirmado no dicionário de dados original (SchemaSpy)."
        )
    return (
        "Não identificável com segurança: tabela ausente do dicionário de dados "
        "original do SALIC. Presume-se sistema SALIC pelo schema bronze/prefixo "
        f"'{esc(entry['prefixo'])}', mas isso é inferência, não confirmação."
    )


def _columns_table_html(entry: dict[str, Any]) -> str:
    rows = []
    for col in entry["colunas"]:
        tipo = col.get("tipo_documentado") or "não documentado"
        tam = col.get("tamanho_documentado")
        tipo_str = f"{esc(tipo)}" + (f" ({esc(tam)})" if tam else "")

        if col.get("ausente_do_perfil_atual"):
            nulos = vazios = distintos = "sem perfil"
        else:
            nulos = f"{_fmt_num(col.get('null_count'))} ({_fmt_pct(col.get('null_pct'))})"
            vazios = _fmt_num(col.get("empty_count"))
            dist = col.get("distinct_count")
            fonte = col.get("distinct_count_fonte")
            distintos = _fmt_num(dist) + (" (estimado)" if fonte == "estimado_pg_stats" else "")

        flags = []
        if col.get("e_chave_primaria_documentada"):
            flags.append("PK")
        if col.get("e_chave_estrangeira_documentada"):
            flags.append("FK")
        chave = ", ".join(flags) or "&mdash;"

        significado = esc(col.get("comentario_original") or "[não documentado]")

        if col.get("value_frequency"):
            vals = [
                f"{esc(display_value(col['name'], v['value']))} ({v['freq']}x)"
                for v in col["value_frequency"][:6]
            ]
            exemplo = "; ".join(vals)
        elif col.get("min_text") is not None or col.get("max_text") is not None:
            exemplo = (
                f"min: {esc(display_value(col['name'], col.get('min_text')))} / "
                f"max: {esc(display_value(col['name'], col.get('max_text')))}"
            )
        else:
            exemplo = "&mdash;"

        rows.append(
            f"<tr><td>{esc(col['name'])}</td><td>{tipo_str}</td><td>{nulos}</td>"
            f"<td>{vazios}</td><td>{distintos}</td><td>{chave}</td>"
            f"<td>{significado}</td><td>{exemplo}</td></tr>"
        )

    headers = ["Coluna", "Tipo/Tamanho", "Nulos", "Vazios", "Distintos", "Chave", "Significado", "Exemplo/Domínio observado"]
    thead = "".join(f"<th>{h}</th>" for h in headers)
    return f'<table class="gh-table"><thead><tr>{thead}</tr></thead><tbody>{"".join(rows)}</tbody></table>'


def _table_card_html(entry: dict[str, Any]) -> str:
    parts = [f'<article class="gh-card" id="{esc(entry["nome_tabela"])}">']
    parts.append(f'<h2 class="gh-card__title">{esc(entry["nome_tabela"])}</h2>')
    if entry.get("nome_tabela_origem"):
        parts.append(f'<p class="gh-card__subtitle">Nome original: {esc(entry["nome_tabela_origem"])}</p>')
    parts.append(f"<p>{esc(entry.get('descricao') or 'Descrição não documentada.')}</p>")

    parts.append(
        render_kv_table(
            [
                ("Domínio/assunto", esc(DOMINIOS.get(entry["prefixo"], entry["prefixo"]))),
                ("Quantidade de registros (bronze, hoje)", _fmt_num(entry["linhas_atuais_bronze"])),
                ("Quantidade de registros (snapshot dicionário original)", _fmt_num(entry.get("linhas_snapshot_dicionario_original"))),
                ("Quantidade de colunas", str(entry["quantidade_colunas"])),
                ("Origem dos dados", _origem_dados(entry)),
                ("Periodicidade/atualização", "Não documentada na origem nem inferível deste levantamento."),
                ("Chave primária (documentada)", esc(", ".join(entry.get("chave_primaria_documentada") or []) or "Não documentada.")),
                ("Presente no dicionário original", "Sim" if entry["presente_no_dicionario_original"] else "Não"),
            ]
        )
    )

    if entry.get("regras_de_negocio_documentadas"):
        items = [f"{c['nome']}: {c['expressao']}" for c in entry["regras_de_negocio_documentadas"]]
        parts.append("<h3>Regras de negócio documentadas (check constraints)</h3>")
        parts.append(render_bullet_list(items))

    fks = [c for c in entry["colunas"] if c.get("e_chave_estrangeira_documentada")]
    if fks:
        items = []
        for c in fks[:MAX_REL_SHOWN]:
            items.extend(c.get("referencias_documentadas") or [])
        if len(fks) > MAX_REL_SHOWN:
            items.append(f"... e mais {len(fks) - MAX_REL_SHOWN} coluna(s) com FK documentada.")
        parts.append("<h3>Chaves estrangeiras / relacionamentos</h3>")
        parts.append(render_bullet_list(items or ["Ver colunas individuais abaixo."]))

    if entry.get("observacao"):
        parts.append(
            f'<div class="gh-callout"><strong>{render_badge("Observações e limitações", "accent")}</strong>'
            f"{render_bullet_list(entry['observacao'])}</div>"
        )

    parts.append("<h3>Colunas</h3>")
    parts.append(_columns_table_html(entry))
    parts.append("</article>")
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=str, default=None)
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args()

    merged: dict[str, Any] = json.loads(MERGED_PATH.read_text())
    if args.tables:
        wanted = [t.strip() for t in args.tables.split(",")]
        merged = {k: v for k, v in merged.items() if k in wanted}

    by_prefix: dict[str, list[dict[str, Any]]] = {}
    for entry in merged.values():
        by_prefix.setdefault(entry["prefixo"], []).append(entry)

    toc_parts = ['<input type="search" id="gh-search" placeholder="Buscar tabela...">']
    content_parts = []

    for prefix in SCHEMA_ORDER:
        entries = by_prefix.get(prefix)
        if not entries:
            continue
        entries = sorted(entries, key=lambda e: e["nome_tabela"])

        toc_parts.append(f'<div class="gh-toc__domain">{esc(prefix)} ({len(entries)})</div>')
        for e in entries:
            flag_cls = " gh-flagged" if e.get("observacao") else ""
            search_blob = f"{e['nome_tabela']} {e.get('nome_tabela_origem') or ''} {e.get('descricao') or ''}"
            toc_parts.append(
                f'<a class="gh-toc__link{flag_cls}" href="#{esc(e["nome_tabela"])}" '
                f'data-search="{esc(search_blob.lower())}">{esc(e["nome_tabela"])}</a>'
            )

        content_parts.append(
            f'<div class="gh-domain-header"><h1>Domínio: {esc(prefix)}</h1>'
            f"<p>{esc(DOMINIOS.get(prefix, ''))}</p>"
            f"<p>{len(entries)} tabela(s) neste domínio.</p></div>"
        )
        for e in entries:
            content_parts.append(_table_card_html(e))

    body = (
        render_cover(
            "Dicionário de Dados — SALIC",
            "Schema bronze — camada bruta do Sistema de Apoio às Leis de Incentivo à Cultura. "
            "Documento técnico, geração automatizada e reprodutível.",
        )
        + '<div class="gh-layout">'
        + f'<nav class="gh-toc">{"".join(toc_parts)}</nav>'
        + f'<main class="gh-content">{"".join(content_parts)}</main>'
        + "</div>"
        + '<footer class="gh-footer">Gerado automaticamente pelo pipeline scripts/salic_docs — GovHub / SALIC.</footer>'
    )

    html_doc = page_shell(title="Dicionário de Dados — SALIC", body=body)

    out_path = Path(args.out) if args.out else DEFAULT_OUT
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")
    print(f"HTML gerado: {out_path} ({len(merged)} tabelas)")


if __name__ == "__main__":
    main()
