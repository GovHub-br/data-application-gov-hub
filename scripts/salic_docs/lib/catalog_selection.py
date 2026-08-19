"""Seleção de tabelas "principais" (núcleo de negócio) e perguntas de
exemplo, compartilhada pelos geradores de Catálogo de Dados (DOCX e HTML) —
06_generate_catalogo_dados.py e 08_generate_html_catalogo.py. Seleção por
palavra-chave no nome, não hardcoded por tabela específica, para não quebrar
se a base mudar.
"""

from __future__ import annotations

import re
from typing import Any

CORE_KEYWORDS = {
    "sac": [
        "preprojeto", "captacao", "edital", "parecer", "avaliacao",
        "prestacaocontas", "orcamento", "documentosprojeto", "documentosproponente",
        "fluxosprojeto", "ordembancaria", "empenhoprojeto", "abrangencia",
        "distribuicao_avaliacao", "aprovacao", "patrocinio", "incentivo",
    ],
    "tabelas": ["cadastro", "pessoa", "orgaos", "localidades"],
    "agentes": ["agentes", "nomes", "endereco", "telefones", "documentos"],
}
CORE_CAP_PER_DOMAIN = {"sac": 14, "tabelas": 6, "agentes": 6}

QUESTION_TEMPLATES: list[tuple[re.Pattern, list[str]]] = [
    (re.compile(r"preprojeto|projeto"), [
        "Quais projetos culturais estão registrados e qual o status de cada um?",
        "Quantos projetos foram propostos em um determinado período?",
    ]),
    (re.compile(r"captacao"), [
        "Quanto foi captado em recursos por projeto/proponente?",
        "Qual o volume de captação ao longo do tempo?",
    ]),
    (re.compile(r"edital"), [
        "Quais editais estão cadastrados e quais projetos estão vinculados a cada um?",
    ]),
    (re.compile(r"parecer|avaliacao"), [
        "Quais pareceres/avaliações foram emitidos para um projeto ou proponente?",
    ]),
    (re.compile(r"prestacaocontas"), [
        "Quais projetos já prestaram contas e qual a situação da prestação?",
    ]),
    (re.compile(r"pessoa|agente|proponente"), [
        "Quem são os proponentes/agentes cadastrados e quais seus dados de identificação?",
    ]),
    (re.compile(r"orgao"), [
        "Quais órgãos estão cadastrados e como se relacionam entre si?",
    ]),
    (re.compile(r"localidade|municipio|logradouro|endereco"), [
        "Em quais localidades os projetos/agentes estão registrados?",
    ]),
]
DEFAULT_QUESTIONS = ["Quais registros existem nesta tabela e com que frequência aparecem?"]


def friendly_name(entry: dict[str, Any]) -> str:
    origem = entry.get("nome_tabela_origem")
    base = origem if origem else entry["nome_tabela"].split("__", 1)[-1]
    spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", base).replace("_", " ")
    return spaced.strip().title()


def questions_for(entry: dict[str, Any]) -> list[str]:
    name = (entry.get("nome_tabela_origem") or entry["nome_tabela"]).lower()
    desc = (entry.get("descricao") or "").lower()
    haystack = f"{name} {desc}"
    qs: list[str] = []
    for pattern, questions in QUESTION_TEMPLATES:
        if pattern.search(haystack):
            qs.extend(questions)
    return qs or DEFAULT_QUESTIONS


def related_tables(entry: dict[str, Any]) -> list[str]:
    return sorted(
        {
            m.group(1)
            for c in entry["colunas"]
            for ref in (c.get("referencias_documentadas") or [])
            if (m := re.search(r"references\s+(\S+)\.", ref))
        }
    )


def _is_core_candidate(entry: dict[str, Any]) -> bool:
    prefix = entry["prefixo"]
    if prefix not in CORE_KEYWORDS:
        return False
    if entry.get("observacao"):
        return False
    if not entry["linhas_atuais_bronze"]:
        return False
    name = (entry.get("nome_tabela_origem") or entry["nome_tabela"]).lower()
    return any(kw in name for kw in CORE_KEYWORDS[prefix])


def select_core(entries: list[dict[str, Any]]) -> set[str]:
    by_prefix: dict[str, list[dict[str, Any]]] = {}
    for e in entries:
        if _is_core_candidate(e):
            by_prefix.setdefault(e["prefixo"], []).append(e)
    core: set[str] = set()
    for prefix, cands in by_prefix.items():
        cands.sort(key=lambda e: e["linhas_atuais_bronze"], reverse=True)
        cap = CORE_CAP_PER_DOMAIN.get(prefix, 0)
        for e in cands[:cap]:
            core.add(e["nome_tabela"])
    return core
