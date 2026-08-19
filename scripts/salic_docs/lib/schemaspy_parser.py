"""Parser do dicionário de dados original do SALIC (export SchemaSpy 7.0.2
do SQL Server de origem).

Cada `tables/<Tabela>.html` documenta uma tabela: descrição, contagem de
linhas (snapshot da extração), colunas (tipo, tamanho, nulos, default,
comentário, PK/FK), índices e check constraints. Este módulo extrai isso
para dicts simples, chaveados com o mesmo padrão `<prefixo>__<tabela>` usado
no schema `bronze` do data warehouse, para permitir o cruzamento direto.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup, Tag

# Nome da pasta do dicionário original -> prefixo usado no bronze.
PREFIX_BY_DIR = {
    "SAC": "sac",
    "Agentes": "agentes",
    "Tabelas": "tabelas",
    "ControleDeAcesso": "controledeacesso",
    "BDCORPORATIVO": "bdcorporativo",
}


def _box_body(soup: BeautifulSoup, *, heading_id: str | None = None, heading_text: str | None = None) -> Tag | None:
    """Acha a `div.box-body` da seção cujo `h3.box-title` bate por id ou texto."""
    headings = soup.find_all("h3", class_="box-title")
    for h in headings:
        if heading_id is not None and h.get("id") != heading_id:
            continue
        if heading_text is not None and h.get_text(strip=True) != heading_text:
            continue
        box = h.find_parent("div", class_="box")
        if box is None:
            continue
        body = box.find("div", class_="box-body")
        if body is not None:
            return body
    return None


def _cell_text(td: Tag) -> str:
    return td.get_text(strip=True)


def _relationship_refs(td: Tag) -> list[str]:
    """Extrai as frases de relacionamento (título completo do SchemaSpy,
    ex.: 'Abrangencia.idProjeto references PreProjeto.idPreProjeto via
    FK_Abrangencia_PreProjeto') de uma célula Children/Parents."""
    refs = []
    for cell in td.select("td[title]"):
        title = cell.get("title", "").strip()
        if title:
            refs.append(title)
    return refs


def _parse_columns(box_body: Tag | None) -> list[dict[str, Any]]:
    if box_body is None:
        return []
    table = box_body.find("table")
    if table is None:
        return []
    tbody = table.find("tbody") or table
    columns = []
    for tr in tbody.find_all("tr", recursive=False):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 9:
            continue
        name_cell = tds[0]
        span = name_cell.find("span", id=True)
        name = span.get_text(strip=True) if span else _cell_text(name_cell)
        classes = name_cell.get("class", []) or []
        columns.append(
            {
                "name": name,
                "type": _cell_text(tds[1]),
                "size": _cell_text(tds[2]),
                "nullable": _cell_text(tds[3]) == "√",
                "auto": _cell_text(tds[4]) == "√",
                "default": _cell_text(tds[5]) or None,
                "is_primary_key": "primaryKey" in classes,
                "is_foreign_key": "foreignKey" in classes,
                "is_indexed": "indexedColumn" in classes,
                "children_refs": _relationship_refs(tds[6]),
                "parent_refs": _relationship_refs(tds[7]),
                "comment": _cell_text(tds[8]) or None,
            }
        )
    return columns


def _parse_indexes(box_body: Tag | None) -> tuple[list[str], list[dict[str, Any]]]:
    """Retorna (colunas_da_pk, lista_de_indices) a partir da seção Indexes."""
    if box_body is None:
        return [], []
    table = box_body.find("table")
    if table is None:
        return [], []
    tbody = table.find("tbody") or table
    pk_columns: list[str] = []
    indexes = []
    for tr in tbody.find_all("tr", recursive=False):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 4:
            continue
        name = _cell_text(tds[0])
        idx_type = _cell_text(tds[1])
        columns_raw = _cell_text(tds[3])
        columns = [c.strip() for c in columns_raw.split("+")]
        if "primaryKey" in (tds[0].get("class", []) or []) or idx_type == "Primary key":
            pk_columns = columns
        indexes.append({"name": name, "type": idx_type, "columns": columns})
    return pk_columns, indexes


def _parse_check_constraints(box_body: Tag | None) -> list[dict[str, str]]:
    if box_body is None:
        return []
    table = box_body.find("table")
    if table is None:
        return []
    tbody = table.find("tbody") or table
    out = []
    for tr in tbody.find_all("tr", recursive=False):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            continue
        out.append({"name": _cell_text(tds[0]), "expression": _cell_text(tds[1])})
    return out


def parse_table_page(html_path: Path) -> dict[str, Any]:
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")

    h1 = soup.find("h1")
    table_name = h1.get_text(strip=True) if h1 else html_path.stem

    record_span = soup.find("span", id="recordNumber")
    row_count_snapshot: int | None = None
    if record_span is not None:
        raw = record_span.get_text(strip=True).replace(",", "").replace(".", "")
        if raw.isdigit():
            row_count_snapshot = int(raw)

    desc_body = _box_body(soup, heading_id="Description")
    description = None
    if desc_body is not None:
        p = desc_body.find("p")
        text = p.get_text(strip=True) if p else desc_body.get_text(strip=True)
        description = text or None

    columns = _parse_columns(_box_body(soup, heading_id="Columns"))
    pk_columns, indexes = _parse_indexes(_box_body(soup, heading_id="Indexes"))
    check_constraints = _parse_check_constraints(
        _box_body(soup, heading_text="Check Constraints")
    )

    return {
        "table_name": table_name,
        "row_count_snapshot": row_count_snapshot,
        "description": description,
        "columns": columns,
        "primary_key": pk_columns,
        "indexes": indexes,
        "check_constraints": check_constraints,
    }


def parse_schema_dir(schema_dir: Path) -> dict[str, dict[str, Any]]:
    tables_dir = schema_dir / "tables"
    out: dict[str, dict[str, Any]] = {}
    if not tables_dir.exists():
        return out
    for html_path in sorted(tables_dir.glob("*.html")):
        parsed = parse_table_page(html_path)
        key = parsed["table_name"].lower()
        out[key] = parsed
    return out


def parse_dictionary(root_dir: Path) -> dict[str, dict[str, Any]]:
    """Faz parse das 5 pastas do dicionário original e retorna um dict
    chaveado por `<prefixo>__<tabela_minuscula>`, no mesmo padrão de nomes
    usado no schema bronze."""
    out: dict[str, dict[str, Any]] = {}
    for dir_name, prefix in PREFIX_BY_DIR.items():
        schema_dir = root_dir / dir_name
        parsed = parse_schema_dir(schema_dir)
        for table_key, table_data in parsed.items():
            out[f"{prefix}__{table_key}"] = {**table_data, "source_prefix": prefix}
    return out


if __name__ == "__main__":
    import sys

    root = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if root is None:
        raise SystemExit("uso: python schemaspy_parser.py <caminho_do_dicionario>")
    result = parse_dictionary(root)
    print(json.dumps({"total_tabelas": len(result)}, ensure_ascii=False))
