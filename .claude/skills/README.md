# Skills do repositório

Dez skills versionadas aqui dentro. Quem clona o repositório recebe todas — não
há nada a instalar, nenhum marketplace a registrar, nenhum comando a rodar.

Você não precisa chamá-las pelo nome. O Claude aciona sozinho quando o pedido
casa com a `description` do `SKILL.md`. Para forçar, cite: *"use a skill
`revisao-semanal`"*.

## O que tem aqui

### Escritas para este repositório

| Skill | O que faz |
|---|---|
| [`govhub-pipeline-guide-minc`](govhub-pipeline-guide-minc/) | Caminho completo de uma fonte nova: cliente de API, DAG, modelos dbt de bronze a gold, testes. Tem também "onde mexer por sintoma" e a tabela de erros comuns |
| [`revisao-semanal`](revisao-semanal/) | Revisão da semana — commits na main e nas outras branches, PRs, issues fechadas e abertas — dizendo o que foi feito e o que não andou |
| [`abrir-pr`](abrir-pr/) | Escreve o PR no formato do `PULL_REQUEST_TEMPLATE.md`, a partir dos commits da branch |
| [`to-issues-minc`](to-issues-minc/) | Escreve issues no formato dos seis formulários, já com a label certa |

### Copiadas do GovHub-skills

| Skill | O que faz |
|---|---|
| [`commit-smart`](commit-smart/) | Mensagem de commit no padrão do projeto, a partir do diff. Adaptada: aponta para o `COMMIT_TEMPLATE.md` daqui |
| [`accountability-report`](accountability-report/) | Relatório de prestação de contas de um período, em Markdown, HTML e PDF |
| [`github-actions-creator`](github-actions-creator/) | Escreve workflows do GitHub Actions |
| [`architecture-decision-records`](architecture-decision-records/) | Registra decisão técnica com contexto e alternativas descartadas |
| [`crafting-effective-readmes`](crafting-effective-readmes/) | Escreve e atualiza README |
| [`mermaid-diagram-specialist`](mermaid-diagram-specialist/) | Diagramas de pipeline e de dependência entre tabelas |

Cada uma dessas seis tem um bloco `PROCEDÊNCIA` logo depois do frontmatter, com o
commit de origem no [GovHub-skills](https://github.com/GovHub-br/GovHub-skills).

## Atualizar uma skill copiada

São cópias, não instalação por marketplace: correção feita lá em cima **não chega
sozinha aqui**. Foi uma escolha — em troca, a equipe recebe tudo ao clonar, sem
passo de instalação. Para atualizar:

```bash
git clone --depth 1 https://github.com/GovHub-br/GovHub-skills.git /tmp/gs
rsync -a --exclude node_modules --exclude evals /tmp/gs/05-qualidade-testes/commit-smart/ .claude/skills/commit-smart/
```

Depois recoloque o bloco `PROCEDÊNCIA` com o novo commit, e reaplique as
adaptações locais se houver — a `commit-smart` tem uma seção "Neste repositório"
que a cópia crua apaga.

## Criar uma skill nova

Pasta com `SKILL.md`, e o `name:` do frontmatter **idêntico ao nome da pasta**,
em kebab-case minúsculo. A `description` é o que decide se a skill é acionada na
hora certa: diga *quando* usar, com os gatilhos que a pessoa realmente digita.

Conteúdo pesado vai para `references/`, e scripts para `scripts/`, para o
`SKILL.md` continuar sendo o mapa e não o território — é o que mantém o custo de
contexto baixo.

**Onde a skill deve morar:** se o conhecimento dela sobrevive a um `git mv` neste
repositório, ele pertence ao GovHub-skills, não aqui. Uma skill que cita
`dbt/minc` ou `plugins/cliente_*.py` quebra junto com um refactor daqui, e o
mesmo PR conserta as duas coisas — essa mora aqui. Uma que fala de Postgres em
geral serve a outros projetos e desperdiça ficando só neste.

Antes de promover uma daqui para lá, rode o validador de lá:

```bash
./scripts/validar-plugins.sh
```
