"""Gera o Catálogo de Dados em HTML (identidade visual GovHub real) a partir
de output/merged.json — espelha o conteúdo do DOCX gestor
(06_generate_catalogo_dados.py), com cards para as tabelas principais
(sombra, hover) e tabela resumo para a cauda longa.

Uso:
    poetry run python scripts/salic_docs/08_generate_html_catalogo.py [--tables t1,t2] [--out caminho.html]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.salic_docs.lib.catalog_selection import (
    friendly_name,
    questions_for,
    related_tables,
    select_core,
)
from scripts.salic_docs.lib.html_common import (
    esc,
    page_shell,
    render_badge,
    render_bullet_list,
    render_cover,
)
from scripts.salic_docs.lib.semantics import DOMINIOS

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
MERGED_PATH = OUTPUT_DIR / "merged.json"
FINAL_DIR = Path(__file__).resolve().parents[2] / "dbt" / "minc" / "docs" / "salic"
DEFAULT_OUT = FINAL_DIR / "catalogo_dados_salic.html"
SCHEMA_ORDER = ["sac", "tabelas", "agentes", "controledeacesso", "bdcorporativo"]

EXTRA_CSS = """
.gh-core-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 20px; }
.gh-core-card { background: var(--bg-white); border-radius: var(--radius-md); padding: 22px;
  box-shadow: var(--shadow-md); transition: var(--transition-normal); scroll-margin-top: 16px; }
.gh-core-card:hover { box-shadow: var(--shadow-xl); transform: translateY(-2px); }
.gh-core-card__fields { font-size: 0.78rem; color: var(--text-muted); margin: 8px 0; }
"""


def _core_card_html(entry: dict[str, Any]) -> str:
    campos = ", ".join(c["name"] for c in entry["colunas"][:12])
    fk_tables = related_tables(entry)
    parts = [f'<div class="gh-core-card" id="{esc(entry["nome_tabela"])}">']
    parts.append(f'<h3 class="gh-card__title">{esc(friendly_name(entry))}</h3>')
    parts.append(f'<p class="gh-card__subtitle">Nome técnico: {esc(entry["nome_tabela"])}</p>')
    parts.append(f"<p>{esc(entry.get('descricao') or 'Descrição não documentada na origem.')}</p>")
    parts.append(f'<p class="gh-core-card__fields"><strong>Principais campos:</strong> {esc(campos)}</p>')
    if fk_tables:
        parts.append(f'<p class="gh-core-card__fields"><strong>Relaciona-se com:</strong> {esc(", ".join(fk_tables))}</p>')
    parts.append("<p><strong>Perguntas que esta tabela pode ajudar a responder:</strong></p>")
    parts.append(render_bullet_list(questions_for(entry)))
    registros_txt = f"{entry['linhas_atuais_bronze']:,}".replace(",", ".") + " registros"
    parts.append(f"<p>{render_badge(registros_txt)}</p>")
    if entry.get("observacao"):
        parts.append(f'<div class="gh-callout">{render_bullet_list(entry["observacao"])}</div>')
    parts.append("</div>")
    return "\n".join(parts)


def _rest_table_html(entries: list[dict[str, Any]]) -> str:
    rows = []
    for e in entries:
        desc = e.get("descricao") or "[não documentado]"
        desc = desc if len(desc) <= 220 else desc[:219] + "…"
        obs = "; ".join(e.get("observacao") or [])
        obs = (obs[:150] + "…") if len(obs) > 150 else (obs or "&mdash;")
        registros = f"{e['linhas_atuais_bronze']:,}".replace(",", ".")
        rows.append(
            f"<tr><td>{esc(friendly_name(e))}</td><td>{esc(e['nome_tabela'])}</td>"
            f"<td>{esc(desc)}</td><td>{registros}</td><td>{esc(obs)}</td></tr>"
        )
    headers = "".join(f"<th>{h}</th>" for h in ["Nome amigável", "Nome técnico", "O que representa", "Registros", "Observações"])
    return f'<table class="gh-table"><thead><tr>{headers}</tr></thead><tbody>{"".join(rows)}</tbody></table>'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=str, default=None)
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args()

    merged: dict[str, Any] = json.loads(MERGED_PATH.read_text())
    if args.tables:
        wanted = [t.strip() for t in args.tables.split(",")]
        merged = {k: v for k, v in merged.items() if k in wanted}

    entries = list(merged.values())
    core_names = select_core(entries)

    by_prefix: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        by_prefix.setdefault(entry["prefixo"], []).append(entry)

    toc_parts = ['<input type="search" id="gh-search" placeholder="Buscar tabela...">']
    content_parts = [
        '<div class="gh-card">'
        "<h2>Como usar este catálogo</h2>"
        "<p>Este documento descreve, em linguagem não técnica, os dados disponíveis na base "
        "bruta (bronze) do SALIC. Cada domínio agrupa tabelas com um mesmo propósito de "
        "negócio. As tabelas mais relevantes para consulta recebem um card detalhado, com "
        "exemplos de perguntas que podem ser respondidas; as demais aparecem em formato resumido.</p>"
        '<div class="gh-callout">Atenção: esta é a camada bruta (bronze), réplica do sistema de '
        "origem sem tratamento. Pode conter duplicidades, tabelas de backup/teste e "
        "inconsistências, sinalizadas nas observações de cada tabela.</div>"
        "</div>"
    ]

    for prefix in SCHEMA_ORDER:
        domain_entries = by_prefix.get(prefix)
        if not domain_entries:
            continue
        core = [e for e in domain_entries if e["nome_tabela"] in core_names]
        rest = [e for e in domain_entries if e["nome_tabela"] not in core_names]

        toc_parts.append(f'<div class="gh-toc__domain">{esc(prefix)} ({len(domain_entries)})</div>')
        for e in sorted(domain_entries, key=lambda x: x["nome_tabela"]):
            flag_cls = " gh-flagged" if e.get("observacao") else ""
            search_blob = f"{e['nome_tabela']} {friendly_name(e)} {e.get('descricao') or ''}"
            toc_parts.append(
                f'<a class="gh-toc__link{flag_cls}" href="#{esc(e["nome_tabela"])}" '
                f'data-search="{esc(search_blob.lower())}">{esc(friendly_name(e))}</a>'
            )

        content_parts.append(
            f'<div class="gh-domain-header"><h1>Domínio: {esc(prefix)}</h1>'
            f"<p>{esc(DOMINIOS.get(prefix, ''))}</p>"
            f"<p>{len(domain_entries)} conjunto(s) de dados neste domínio.</p></div>"
        )
        if core:
            content_parts.append("<h2>Tabelas principais</h2>")
            content_parts.append('<div class="gh-core-grid">')
            for e in sorted(core, key=lambda x: x["linhas_atuais_bronze"], reverse=True):
                content_parts.append(_core_card_html(e))
            content_parts.append("</div>")
        if rest:
            content_parts.append("<h2>Demais tabelas do domínio (resumo)</h2>")
            content_parts.append(_rest_table_html(sorted(rest, key=lambda x: x["nome_tabela"])))

    body = (
        render_cover(
            "Catálogo de Dados — SALIC",
            "O que existe na base do SALIC e o que cada conjunto de dados representa. "
            "Visão de negócio — schema bronze (dados brutos).",
        )
        + '<div class="gh-layout">'
        + f'<nav class="gh-toc">{"".join(toc_parts)}</nav>'
        + f'<main class="gh-content">{"".join(content_parts)}</main>'
        + "</div>"
        + '<footer class="gh-footer">Gerado automaticamente pelo pipeline scripts/salic_docs — GovHub / SALIC.</footer>'
    )

    html_doc = page_shell(title="Catálogo de Dados — SALIC", body=body, extra_css=EXTRA_CSS)

    out_path = Path(args.out) if args.out else DEFAULT_OUT
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")
    print(f"HTML gerado: {out_path} ({len(merged)} tabelas, {len(core_names)} curadas)")


if __name__ == "__main__":
    main()
