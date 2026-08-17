"""
build.py: renderiza o site em docs-pages/site/.

Não usa rede, não usa banco, não usa dbt. Consome apenas o acervo em
`src/_data/` e a curadoria em `src/dominios.yml` — por isso o CI consegue
montar o site sem VPN e sem credencial nenhuma.

O build falha de propósito em link interno quebrado: é o tipo de erro que passa
batido numa revisão humana e só aparece para quem visita.

Uso:  python -m tooling.build
"""

from __future__ import annotations

import re
import shutil
import sys
from datetime import date

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from tooling import dados, linhagem
from tooling.common import ASSETS_DIR, SITE_DIR, TEMPLATES_DIR, log

PAGINAS = [
    ("index.html", "index.html.j2", "Visão geral"),
    ("gestao.html", "gestao.html.j2", "Para quem acompanha"),
    ("tecnico.html", "tecnico.html.j2", "Para quem constrói"),
    ("fontes.html", "fontes.html.j2", "De onde vêm os dados"),
]

RE_HREF = re.compile(r'href="(?!https?:|#|mailto:)([^"#]+)')


def _ambiente() -> Environment:
    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=True,
        undefined=StrictUndefined,  # variável ausente vira erro, não string vazia
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["milhar"] = lambda n: f"{n:,}".replace(",", ".")
    return env


def _verificar_links(paginas: dict[str, str]) -> list[str]:
    """Devolve a lista de links internos que apontam para página inexistente."""
    existentes = set(paginas) | {"assets/tema.css"}
    quebrados = []
    for nome, html in paginas.items():
        for alvo in RE_HREF.findall(html):
            alvo = alvo.split("?")[0]
            if alvo and alvo not in existentes:
                quebrados.append(f"{nome} → {alvo}")
    return quebrados


def main() -> int:
    contexto = dados.montar()
    env = _ambiente()

    # A linhagem de cada gold é gerada aqui e entregue pronta ao template —
    # template não calcula, template exibe.
    for dominio in contexto["dominios"]:
        for gold in dominio["golds"]:
            gold["linhagem"] = linhagem.desenhar(gold["nome"], contexto["modelos"])

    base = {
        **contexto,
        "gerado_em": date.today().isoformat(),
        "paginas": PAGINAS,
    }

    renderizadas: dict[str, str] = {}
    for arquivo, template, titulo in PAGINAS:
        renderizadas[arquivo] = env.get_template(template).render(
            **base, pagina_atual=arquivo, titulo=titulo
        )

    for dominio in contexto["dominios"]:
        arquivo = f"dominio-{dominio['slug']}.html"
        renderizadas[arquivo] = env.get_template("dominio.html.j2").render(
            **base, pagina_atual=arquivo, titulo=dominio["rotulo"], dominio=dominio
        )

    quebrados = _verificar_links(renderizadas)
    if quebrados:
        for q in quebrados:
            log.error("link quebrado: %s", q)
        raise SystemExit(f"{len(quebrados)} link(s) interno(s) quebrado(s)")

    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_DIR.mkdir(parents=True)
    for arquivo, html in renderizadas.items():
        (SITE_DIR / arquivo).write_text(html, encoding="utf-8")
    shutil.copytree(ASSETS_DIR, SITE_DIR / "assets")

    m = contexto["metricas"]
    log.info(
        "site em %s — %d páginas, %d modelos, %d DAGs, %d%% dos modelos descritos",
        SITE_DIR.name,
        len(renderizadas),
        m["modelos"],
        m["dags"],
        m["cobertura_descricao"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
