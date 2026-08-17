"""
linhagem.py: desenha o caminho que constrói cada tabela gold, da fonte até ela.

O SVG é gerado aqui, em Python puro — sem Mermaid, sem Node, sem navegador. O
`data-application-cidades` usa `mermaid-cli` e mantém um cache de SVG por hash
justamente porque a renderização precisa de Chrome; gerando o SVG direto, esse
problema não existe e não há nada para cachear.

O grafo vem dos `ref()` e `source()` do próprio SQL, coletados em dbt.json. Não
é desenhado à mão e não sai do lugar quando um modelo muda.
"""

from __future__ import annotations

from html import escape
from typing import Any

ORDEM = ["source", "bronze", "silver", "gold", "views", "outros"]

LARGURA_CAIXA = 168
ALTURA_CAIXA = 40
GAP_X = 76
GAP_Y = 16
MARGEM = 16
TOPO_ROTULO = 26

No = dict[str, str]
Posicao = dict[str, tuple[int, int]]


def _ancestrais(alvo: str, por_nome: dict[str, dict[str, Any]]) -> set[str]:
    """Todos os modelos que o alvo consome, direta ou indiretamente."""
    vistos: set[str] = set()
    fila = [alvo]
    while fila:
        atual = fila.pop()
        if atual in vistos:
            continue
        vistos.add(atual)
        fila.extend(por_nome.get(atual, {}).get("depende_de", []))
    return vistos


def _grafo(
    alvo: str, por_nome: dict[str, dict[str, Any]]
) -> tuple[dict[str, No], list[tuple[str, str]]]:
    """Monta nós e arestas da linhagem. Sources entram como nós próprios."""
    nos: dict[str, No] = {}
    arestas: list[tuple[str, str]] = []

    for nome in _ancestrais(alvo, por_nome):
        modelo = por_nome.get(nome)
        if not modelo:
            continue
        nos[nome] = {"rotulo": nome, "camada": modelo["camada"]}
        for src in modelo.get("sources", []):
            nos[f"src:{src}"] = {"rotulo": src, "camada": "source"}

    for chave, no in list(nos.items()):
        modelo = por_nome.get(no["rotulo"])
        if not modelo or chave.startswith("src:"):
            continue
        arestas.extend((pai, chave) for pai in modelo.get("depende_de", []) if pai in nos)
        arestas.extend((f"src:{src}", chave) for src in modelo.get("sources", []))

    return nos, arestas


def _layout(nos: dict[str, No]) -> tuple[Posicao, list[str], int, int]:
    """Distribui os nós em colunas por camada. Devolve posições e dimensões."""
    colunas: dict[str, list[str]] = {}
    for chave, no in nos.items():
        colunas.setdefault(no["camada"], []).append(chave)
    for chaves in colunas.values():
        chaves.sort(key=lambda k: nos[k]["rotulo"])

    presentes = [c for c in ORDEM if c in colunas]
    pos: Posicao = {}
    for ix, camada in enumerate(presentes):
        x = MARGEM + ix * (LARGURA_CAIXA + GAP_X)
        for iy, chave in enumerate(colunas[camada]):
            pos[chave] = (x, MARGEM + TOPO_ROTULO + iy * (ALTURA_CAIXA + GAP_Y))

    if not presentes:
        return pos, presentes, 0, 0

    largura = MARGEM * 2 + len(presentes) * LARGURA_CAIXA + (len(presentes) - 1) * GAP_X
    alto = max(len(colunas[c]) for c in presentes)
    altura = MARGEM * 2 + TOPO_ROTULO + alto * (ALTURA_CAIXA + GAP_Y)
    return pos, presentes, largura, altura


def _aresta(pos: Posicao, origem: str, destino: str) -> str:
    x1, y1 = pos[origem]
    x2, y2 = pos[destino]
    x1 += LARGURA_CAIXA
    y1 += ALTURA_CAIXA // 2
    y2 += ALTURA_CAIXA // 2
    meio = (x1 + x2) / 2
    return (
        f'<path d="M {x1} {y1} C {meio} {y1}, {meio} {y2}, {x2} {y2}" '
        f'class="lin-aresta" marker-end="url(#seta)"/>'
    )


def _caixa(no: No, x: int, y: int, alvo: str) -> str:
    destaque = " lin-alvo" if no["rotulo"] == alvo else ""
    rotulo = no["rotulo"]
    if len(rotulo) > 24:
        rotulo = rotulo[:23] + "…"
    meio_x = x + LARGURA_CAIXA // 2
    meio_y = y + ALTURA_CAIXA // 2 + 4
    return (
        f'<g class="lin-no lin-{no["camada"]}{destaque}">'
        f'<rect x="{x}" y="{y}" width="{LARGURA_CAIXA}" height="{ALTURA_CAIXA}" rx="5"/>'
        f'<text x="{meio_x}" y="{meio_y}" text-anchor="middle">'
        f"{escape(rotulo)}</text></g>"
    )


def desenhar(modelo: str, modelos: list[dict[str, Any]]) -> str:
    """Devolve o SVG da linhagem do modelo, ou string vazia se não der para montar."""
    por_nome = {m["nome"]: m for m in modelos}
    if modelo not in por_nome:
        return ""

    nos, arestas = _grafo(modelo, por_nome)
    pos, presentes, largura, altura = _layout(nos)
    if not presentes:
        return ""

    partes = [
        f'<svg viewBox="0 0 {largura} {altura}" width="100%" '
        f'xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-label="Linhagem de {escape(modelo)}" class="linhagem">',
        '<defs><marker id="seta" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
        '<path d="M 0 0 L 10 5 L 0 10 z" fill="var(--traco)"/></marker></defs>',
    ]

    for camada in presentes:
        x = next(px for chave, (px, _) in pos.items() if nos[chave]["camada"] == camada)
        partes.append(
            f'<text x="{x}" y="{MARGEM + 12}" class="lin-camada">{escape(camada)}</text>'
        )

    partes.extend(_aresta(pos, o, d) for o, d in arestas if o in pos and d in pos)
    partes.extend(_caixa(nos[chave], x, y, modelo) for chave, (x, y) in pos.items())
    partes.append("</svg>")
    return "".join(partes)
