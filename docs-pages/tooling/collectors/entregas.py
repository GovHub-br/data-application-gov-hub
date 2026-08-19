"""
entregas.py: histórico de entregas a partir do git e dos pull requests.

Usa `git log` sempre, e `gh` quando disponível. Sem rede ou sem `gh`
autenticado, a coleta segue com o que o git local sabe e marca a origem do
registro — o site não deixa de ser construído por causa disso.

Saída: docs-pages/src/_data/entregas.json
"""

from __future__ import annotations

import json
import re
from typing import Any

from tooling.common import log, run, write_json

# Conventional Commits — é a convenção do repositório, e é o que permite
# separar entrega de manutenção sem ninguém classificar à mão.
RE_CONVENCIONAL = re.compile(
    r"^(?P<tipo>\w+)(?:\((?P<escopo>[^)]+)\))?!?:\s*(?P<texto>.+)$"
)

RELEVANTES = {"feat", "fix", "refactor", "perf"}


def _commits() -> list[dict[str, Any]]:
    # `--no-merges`, e não `--first-parent`. Os pull requests deste repositório
    # entram por merge commit, cujo assunto é "Merge pull request #N from ..." —
    # com `--first-parent` o coletor via só essas linhas e enxergava 17 entregas
    # onde existem 181. O trabalho de verdade está nos commits de dentro.
    bruto = run(
        [
            "git",
            "log",
            "--no-merges",
            "origin/main",
            "--pretty=format:%h\x1f%an\x1f%ad\x1f%s",
            "--date=short",
        ]
    )
    saida = []
    for linha in bruto.splitlines():
        partes = linha.split("\x1f")
        if len(partes) != 4:
            continue
        sha, autor, data, assunto = partes
        m = RE_CONVENCIONAL.match(assunto)
        saida.append(
            {
                "sha": sha,
                "autor": autor,
                "data": data,
                "assunto": assunto,
                "tipo": m.group("tipo") if m else "",
                "escopo": (m.group("escopo") or "") if m else "",
                "texto": m.group("texto") if m else assunto,
            }
        )
    return saida


def _pull_requests() -> list[dict[str, Any]]:
    bruto = run(
        [
            "gh",
            "pr",
            "list",
            "--state",
            "merged",
            "--limit",
            "100",
            "--json",
            "number,title,author,mergedAt,additions,deletions",
        ]
    )
    if not bruto.strip():
        log.warning("sem PRs — `gh` indisponível ou sem rede; seguindo só com o git")
        return []
    try:
        dados = json.loads(bruto)
    except json.JSONDecodeError:
        return []
    return [
        {
            "numero": p["number"],
            "titulo": p["title"],
            "autor": (p.get("author") or {}).get("login", ""),
            "data": (p.get("mergedAt") or "")[:10],
            "linhas": p.get("additions", 0) + p.get("deletions", 0),
        }
        for p in dados
    ]


def coletar() -> dict[str, Any]:
    commits = _commits()
    prs = _pull_requests()

    entregas = [c for c in commits if c["tipo"] in RELEVANTES]

    por_mes: dict[str, int] = {}
    for e in entregas:
        mes = e["data"][:7]
        por_mes[mes] = por_mes.get(mes, 0) + 1

    autores: dict[str, int] = {}
    for c in commits:
        autores[c["autor"]] = autores.get(c["autor"], 0) + 1

    payload = {
        "entregas": entregas[:80],
        "pull_requests": prs,
        "por_mes": dict(sorted(por_mes.items())),
        "autores": dict(sorted(autores.items(), key=lambda kv: -kv[1])),
        "totais": {
            "commits": len(commits),
            "entregas": len(entregas),
            "pull_requests": len(prs),
            "primeiro_commit": commits[-1]["data"] if commits else "",
            "ultimo_commit": commits[0]["data"] if commits else "",
        },
    }
    write_json("entregas", payload)
    return payload
