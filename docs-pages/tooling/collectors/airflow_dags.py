"""
airflow_dags.py: inventário das DAGs de ingestão e dos clientes de API.

Lê o código com `ast`, sem importar Airflow. O parse é estático e roda offline —
importar as DAGs exigiria Airflow instalado, conexão configurada e as Variables
existindo, o que tornaria a coleta impossível de rodar no CI.

Saída: docs-pages/src/_data/airflow.json
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

from tooling.common import DAGS_DIR, PLUGINS_DIR, ROOT_DIR, log, write_json


def _literal(no: ast.AST) -> Any:
    """Avalia um nó como literal Python. Devolve None quando não for constante."""
    try:
        return ast.literal_eval(no)
    except (ValueError, SyntaxError, TypeError):
        return None


def _nome_do_decorador(dec: ast.expr) -> str:
    """Nome do decorador, seja ele `@dag`, `@dag(...)` ou `@modulo.dag(...)`."""
    alvo = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(alvo, ast.Name):
        return alvo.id
    if isinstance(alvo, ast.Attribute):
        return alvo.attr
    return ""


def _argumentos_do_dag(dec: ast.expr, funcao: ast.FunctionDef) -> dict[str, Any]:
    """Lê tags, schedule e a primeira linha da docstring do decorador @dag."""
    docstring = (ast.get_docstring(funcao) or "").strip()
    args: dict[str, Any] = {
        "funcao": funcao.name,
        "tags": [],
        "schedule": "",
        "descricao": docstring.split("\n\n")[0].replace("\n", " "),
    }
    keywords = dec.keywords if isinstance(dec, ast.Call) else []
    for kw in keywords:
        if kw.arg == "tags":
            args["tags"] = _literal(kw.value) or []
        elif kw.arg == "schedule":
            valor = _literal(kw.value)
            # `schedule=get_dynamic_schedule(...)` não é literal: a periodicidade
            # real vem da Variable do Airflow, que a coleta não consulta.
            if valor is None and isinstance(kw.value, ast.Call):
                valor = "dinâmico (schedule_loader)"
            args["schedule"] = str(valor) if valor is not None else ""
    return args


def _decorador_dag(arvore: ast.Module) -> dict[str, Any] | None:
    """Extrai os argumentos do decorador @dag, se o arquivo declarar uma DAG."""
    for no in ast.walk(arvore):
        if not isinstance(no, ast.FunctionDef):
            continue
        for dec in no.decorator_list:
            if _nome_do_decorador(dec) == "dag":
                return _argumentos_do_dag(dec, no)
    return None


def _classes(arvore: ast.Module) -> list[dict[str, str]]:
    """Lista as classes declaradas, com a primeira linha da docstring."""
    saida = []
    for no in arvore.body:
        if isinstance(no, ast.ClassDef):
            doc = (ast.get_docstring(no) or "").strip().split("\n")[0]
            base = ""
            if no.bases and isinstance(no.bases[0], ast.Name):
                base = no.bases[0].id
            saida.append({"nome": no.name, "base": base, "descricao": doc})
    return saida


def _parse(arquivo: Path) -> ast.Module | None:
    try:
        return ast.parse(arquivo.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError as erro:
        log.warning("não consegui parsear %s: %s", arquivo.name, erro)
        return None


def coletar() -> dict[str, Any]:
    dags: list[dict[str, Any]] = []
    ingest_dir = DAGS_DIR / "data_ingest"

    if ingest_dir.exists():
        for arquivo in sorted(ingest_dir.rglob("*_dag.py")):
            arvore = _parse(arquivo)
            if arvore is None:
                continue
            info = _decorador_dag(arvore) or {}
            fonte = arquivo.relative_to(ingest_dir).parts[0]
            dags.append(
                {
                    "arquivo": arquivo.name,
                    "caminho": str(arquivo.relative_to(ROOT_DIR)),
                    "fonte": fonte,
                    "dag_id": info.get("funcao", arquivo.stem),
                    "descricao": info.get("descricao", ""),
                    "tags": info.get("tags", []),
                    "schedule": info.get("schedule", ""),
                }
            )

    clientes: list[dict[str, Any]] = []
    if PLUGINS_DIR.exists():
        for arquivo in sorted(PLUGINS_DIR.glob("cliente_*.py")):
            arvore = _parse(arquivo)
            if arvore is None:
                continue
            for classe in _classes(arvore):
                clientes.append(
                    {
                        "arquivo": arquivo.name,
                        "sistema": arquivo.stem.removeprefix("cliente_"),
                        **classe,
                    }
                )

    por_fonte: dict[str, int] = {}
    for d in dags:
        por_fonte[d["fonte"]] = por_fonte.get(d["fonte"], 0) + 1

    payload = {
        "dags": dags,
        "clientes": clientes,
        "por_fonte": por_fonte,
        "totais": {
            "dags": len(dags),
            "fontes": len(por_fonte),
            "clientes": len(clientes),
        },
    }
    write_json("airflow", payload)
    return payload
