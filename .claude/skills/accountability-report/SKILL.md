---
name: accountability-report
description: >-
  Gera um relatório de prestação de contas de um repositório a partir do histórico
  de commits (git) e do estado atual do código, com saída em Markdown, HTML
  estilizado e PDF (A4, pronto para entrega oficial). Use sempre que o usuário
  pedir um "relatório de prestação de contas", "relatório do que foi feito no
  último ano/período", "resumo das entregas do projeto", "relatório de atividades",
  "o que foi desenvolvido", "report de commits", ou quiser documentar/auditar o
  trabalho de um repositório para gestão, cliente ou órgão — mesmo que não diga
  explicitamente "prestação de contas". Funciona em qualquer repositório
  (front-end, back-end, dados, infra): a skill descobre a stack e a estrutura
  sozinha antes de escrever.
---

<!-- PROCEDÊNCIA
Copiada de GovHub-br/GovHub-skills, pasta 01-govhub/accountability-report
Commit de origem: 8ffa097e6c3e5f63c6de73aca9d976897b1307b0 (2026-08-10)

Cópia, e não instalação por marketplace: a equipe recebe as skills ao
clonar, sem rodar nada. O custo é que correção feita lá em cima não chega
sozinha aqui — para atualizar, recopie a pasta e troque o commit acima.
-->

# Relatório de Prestação de Contas

Este skill produz um relatório que conta, de forma honesta e auditável, **o que foi
entregue em um repositório durante um período** — combinando o que o histórico de
commits revela com o que o código atual de fato contém. O público típico é gestão,
cliente ou um órgão de controle, então o relatório precisa ser legível por quem não
é técnico, mas também rastreável por quem quiser conferir.

O resultado final são três arquivos com o mesmo conteúdo, na raiz do repositório:
`RELATORIO_PRESTACAO_DE_CONTAS_<periodo>.md`, `.html` e `.pdf`.

## Princípio que guia tudo

O relatório precisa ser **fiel ao que realmente aconteceu**. Os commits dizem o que
foi *feito ao longo do tempo*; o código atual diz o que *existe hoje*. As duas coisas
divergem (features removidas, refatoradas, renomeadas), e é por isso que você olha as
duas fontes. Quando um número não puder ser apurado com confiança, diga isso no
relatório em vez de estimar — a credibilidade de uma prestação de contas vem
justamente de não inventar.

## Passo 0 — Entender o pedido

Antes de coletar dados, confirme com o usuário (a não ser que já esteja claro na
conversa):

- **Período** a cobrir (ex.: últimos 12 meses, ano civil, desde o início). Converta
  para datas absolutas. Se o repo for mais novo que o período pedido, cubra desde o
  primeiro commit e registre isso numa nota metodológica — é comum o "último ano"
  coincidir com toda a vida do projeto.
- **Público/nível**: executivo (gestão, sem jargão), técnico detalhado, ou misto
  (resumo executivo + anexo técnico). O misto é o padrão mais seguro para prestação
  de contas e quase sempre a melhor escolha.

Não pergunte mais do que isso — formato (MD/HTML/PDF) já é fixo, e o resto você
descobre lendo o repositório.

## Passo 1 — Analisar o repositório

Leia `references/git_analysis.md` — ele tem os comandos de git prontos para extrair
tudo o que o relatório precisa (totais, commits por mês, autores, tipos de commit,
escopos, nomes de branches de PR). Rode-os e guarde os números.

Em paralelo, **descubra a stack e a estrutura você mesmo**, porque o inventário do
"o que existe hoje" depende inteiramente do tipo de projeto. Não assuma a tecnologia —
verifique. Alguns pontos de partida:

- Arquivos de manifesto na raiz: `package.json`, `pyproject.toml`, `requirements.txt`,
  `go.mod`, `pom.xml`, `Gemfile`, `dbt_project.yml`, `Cargo.toml`. Eles revelam o
  framework e as dependências principais.
- A árvore de diretórios de código-fonte (`src/`, `app/`, `models/`, `pages/`, etc.).
- Para **front-end**: rotas/páginas, componentes (especialmente os compartilhados),
  features por módulo, integrações com APIs, e testes (unit/e2e).
- Para **back-end/API**: endpoints/controllers, serviços, modelos de dados, migrations,
  jobs/filas, integrações.
- Para **dados (dbt/ETL)**: modelos por camada, fontes, testes de qualidade, schedules.

Para nomes amigáveis das entregas (telas, módulos, relatórios), **prefira extrair do
próprio código** — títulos de rota, labels, `COMMENT`/docstrings, nomes de componentes —
em vez de só inferir do nome do arquivo. Foi o que deu profundidade ao relatório: o
nome do arquivo diz `e_cnab_returns_total`, mas o label no código diz "COBRANÇA -
Retorno completo CNAB", e é esse último que a gestão entende.

## Passo 2 — Escrever o relatório em Markdown

Monte o `.md` com esta estrutura (adapte os nomes de seção ao domínio do projeto, mas
mantenha o esqueleto). Esta ordem leva o leitor do panorama ao detalhe:

```
# Relatório de Prestação de Contas — <Nome do Projeto>

**Período coberto:** ... · **Repositório:** ... · **Data de emissão:** ...

> Nota metodológica (se o repo for mais novo que o período, ou houver autores
> duplicados, ou qualquer ressalva sobre os dados).

## 1. Resumo Executivo
   - 1-2 parágrafos em linguagem de negócio: o que o projeto é e o valor entregue.
   - Tabela "Números do período" (commits, PRs, feats, fixes, e os artefatos-chave
     do domínio — telas, endpoints, modelos, testes...).
   - Tabela "Distribuição ao longo do tempo" (mês | commits | marco principal).

## 2. Principais Entregas por Tema
   - Agrupe por tema/funcionalidade de NEGÓCIO, não por tipo de arquivo. Use os
     escopos dos commits (feat(escopo)) e os nomes de branch dos PRs para descobrir
     os temas. Uma subseção por área (ex.: Autenticação, Cobrança, Seguros...).

## 3. Artefatos Entregues (Detalhamento Técnico)
   - Inventário do que existe hoje, adaptado à stack: telas/rotas, componentes,
     endpoints, modelos, testes. Quando a lista for longa e valiosa (ex.: dezenas
     de relatórios ou telas), inclua uma TABELA nominal completa com o nome técnico
     e o nome amigável de cada item — é isso que comprova a entrega.

## 4. Manutenção e Correções
   - Temas recorrentes de fix, em linguagem de negócio.

## 5. Equipe (Colaboradores no Período)
   - Tabela autor | commits. Sinalize (sem consolidar à força) nomes que parecem ser
     a mesma pessoa com configs de git diferentes — isso é honestidade metodológica.

## 6. Conclusão
   - Fecha com os números consolidados e o impacto por área.

*Rodapé: origem dos dados (git log do período X), para auditoria.*
```

Escreva em **português** (a menos que o usuário peça outro idioma). Use tabelas Markdown
de verdade (GFM) — elas viram tabelas estilizadas no HTML/PDF.

## Passo 3 — Gerar HTML e PDF

Os scripts já encapsulam o que foi validado; não reinvente a conversão à mão.

1. **HTML** — o `build_report.mjs` converte o Markdown em HTML autocontido (CSS inline,
   layout A4, capa com tag, títulos em azul, tabelas zebradas, quebras de página
   tratadas). Ele depende do pacote `marked`. Garanta-o assim (instala uma vez no
   diretório do script, não polui o projeto do usuário):

   ```bash
   SKILL_DIR="$HOME/.claude/skills/accountability-report"
   cd "$SKILL_DIR/scripts" && [ -d node_modules/marked ] || (npm init -y >/dev/null 2>&1; npm install marked@12 >/dev/null 2>&1)
   node "$SKILL_DIR/scripts/build_report.mjs" "<caminho>/RELATORIO_....md" "<caminho>/RELATORIO_....html"
   ```

   Passe um terceiro argumento opcional para personalizar a tag da capa, ex.:
   `node build_report.mjs in.md out.html "Prestação de Contas · 2025–2026"`.

2. **PDF** — `html_to_pdf.sh` detecta a ferramenta disponível (Chrome/Chromium headless,
   senão weasyprint, senão wkhtmltopdf) e gera o PDF A4:

   ```bash
   bash "$SKILL_DIR/scripts/html_to_pdf.sh" "<caminho>/RELATORIO_....html" "<caminho>/RELATORIO_....pdf"
   ```

   Se nenhuma ferramenta existir, o script avisa — nesse caso, entregue MD + HTML e
   diga ao usuário que o PDF precisa de uma das ferramentas instaladas.

## Passo 4 — Verificar e entregar

Confirme que o PDF foi de fato gerado (`file <pdf>` deve dizer "PDF document" e o nº de
páginas). Então apresente os três caminhos ao usuário com links relativos clicáveis, um
resumo dos números principais, e aponte qualquer ressalva metodológica (autores
duplicados, período encurtado). Ofereça ajustes de aparência (logo, capa separada,
numeração de rodapé) — são baratos de aplicar regerando o HTML/PDF.
