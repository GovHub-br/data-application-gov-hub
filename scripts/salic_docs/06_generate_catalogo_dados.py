"""Gera o Catálogo de Dados (DOCX gestor) a partir de output/merged.json —
visão orientada a negócio, organizada por domínio, com tratamento mais rico
para um conjunto curado de tabelas centrais (ver lib/catalog_selection.py)
e listagem mais enxuta para a cauda longa de tabelas legadas/técnicas.

Uso:
    poetry run python scripts/salic_docs/06_generate_catalogo_dados.py [--tables t1,t2] [--out caminho.docx]
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
from scripts.salic_docs.lib.docx_common import (
    add_bullet_list,
    add_toc,
    finalize_table,
    fmt_num,
    new_document,
)
from scripts.salic_docs.lib.semantics import DOMINIOS

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
MERGED_PATH = OUTPUT_DIR / "merged.json"
FINAL_DIR = Path(__file__).resolve().parents[2] / "dbt" / "minc" / "docs" / "salic"
DEFAULT_OUT = FINAL_DIR / "catalogo_dados_salic.docx"


def _add_core_entry(doc, entry: dict[str, Any]) -> None:
    doc.add_heading(friendly_name(entry), level=2)
    p = doc.add_paragraph()
    p.add_run(f"Nome técnico: {entry['nome_tabela']}").italic = True

    doc.add_paragraph(entry.get("descricao") or "Descrição não documentada na origem.")

    campos = [c["name"] for c in entry["colunas"]][:12]
    doc.add_paragraph(f"Principais campos disponíveis: {', '.join(campos)}.")

    fk_tables = related_tables(entry)
    if fk_tables:
        doc.add_paragraph(f"Relaciona-se com: {', '.join(fk_tables)}.")

    doc.add_paragraph("Perguntas que esta tabela pode ajudar a responder:")
    add_bullet_list(doc, questions_for(entry))

    doc.add_paragraph(
        f"Volume atual: {fmt_num(entry['linhas_atuais_bronze'])} registro(s) na camada bronze."
    )
    if entry.get("observacao"):
        doc.add_paragraph("Observações e limitações:")
        add_bullet_list(doc, entry["observacao"])
    doc.add_paragraph()


def _add_compact_row(table, entry: dict[str, Any]) -> None:
    row = table.add_row().cells
    row[0].text = friendly_name(entry)
    row[1].text = entry["nome_tabela"]
    desc = entry.get("descricao") or "[não documentado]"
    row[2].text = desc if len(desc) <= 220 else desc[:219] + "…"
    row[3].text = fmt_num(entry["linhas_atuais_bronze"])
    obs = "; ".join(entry.get("observacao") or [])
    row[4].text = (obs[:150] + "…") if len(obs) > 150 else (obs or "—")


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

    doc = new_document(
        "Catálogo de Dados — SALIC",
        "O que existe na base do SALIC e o que cada conjunto de dados representa\n"
        "Visão de negócio — schema bronze (dados brutos)",
    )
    add_toc(doc)

    doc.add_heading("Como usar este catálogo", level=1)
    doc.add_paragraph(
        "Este documento descreve, em linguagem não técnica, os dados disponíveis na base "
        "bruta (bronze) do SALIC. Cada domínio abaixo agrupa tabelas com um mesmo propósito "
        "de negócio. As tabelas mais relevantes para consulta recebem uma descrição "
        "detalhada, com exemplos de perguntas que podem ser respondidas; as demais — em "
        "geral tabelas técnicas, históricas ou de apoio — aparecem em formato resumido."
    )
    doc.add_paragraph(
        "Atenção: esta é a camada bruta (bronze), réplica do sistema de origem sem "
        "tratamento. Pode conter duplicidades, tabelas de backup/teste e inconsistências, "
        "sinalizadas nas observações de cada tabela."
    )
    doc.add_page_break()

    by_prefix: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        by_prefix.setdefault(entry["prefixo"], []).append(entry)

    for prefix in ["sac", "tabelas", "agentes", "controledeacesso", "bdcorporativo"]:
        domain_entries = by_prefix.get(prefix)
        if not domain_entries:
            continue
        doc.add_heading(f"Domínio: {prefix}", level=1)
        doc.add_paragraph(DOMINIOS.get(prefix, ""))
        doc.add_paragraph(f"{len(domain_entries)} conjunto(s) de dados neste domínio.")

        core = [e for e in domain_entries if e["nome_tabela"] in core_names]
        rest = [e for e in domain_entries if e["nome_tabela"] not in core_names]

        if core:
            doc.add_heading("Tabelas principais", level=2)
            for entry in sorted(core, key=lambda e: e["linhas_atuais_bronze"], reverse=True):
                _add_core_entry(doc, entry)

        if rest:
            doc.add_heading("Demais tabelas do domínio (resumo)", level=2)
            table = doc.add_table(rows=1, cols=5)
            for cell, text in zip(
                table.rows[0].cells,
                ["Nome amigável", "Nome técnico", "O que representa", "Registros", "Observações"],
            ):
                cell.text = text
            for entry in sorted(rest, key=lambda e: e["nome_tabela"]):
                _add_compact_row(table, entry)
            finalize_table(table)
        doc.add_page_break()

    out_path = Path(args.out) if args.out else DEFAULT_OUT
    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(out_path))
    print(f"DOCX gerado: {out_path} ({len(merged)} tabelas, {len(core_names)} curadas)")


if __name__ == "__main__":
    main()
