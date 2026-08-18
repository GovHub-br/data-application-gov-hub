# Registro de decisões de arquitetura (ADR)

Decisões técnicas que custaram discussão e que alguém, daqui a seis meses, vai
querer entender antes de desfazer. Cada uma registra o contexto, a decisão, o
motivo, o que foi descartado e o que ficou de consequência.

Um ADR não se edita depois de aceito — se a decisão mudar, escreve-se um novo
que a substitui, e o antigo passa a `Status: substituído por ADR NNNN`.

| # | Decisão |
|---|---|
| [0001](0001-formularios-de-issue-em-yaml.md) | Formulários de issue em YAML, com o texto do cidades |
| [0002](0002-skills-versionadas-no-repositorio.md) | Skills versionadas no repositório, não instaladas por marketplace |
| [0003](0003-labels-por-eixo.md) | Vocabulário de labels organizado por eixo |
| [0004](0004-revisao-semanal-apura-antes-de-narrar.md) | A revisão semanal apura por script e narra por modelo |

Para escrever um novo, existe a skill
[`architecture-decision-records`](../../.claude/skills/architecture-decision-records/).
