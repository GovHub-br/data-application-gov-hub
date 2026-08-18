# ADR 0003 — Vocabulário de labels organizado por eixo

- **Data:** 2026-08-13
- **Status:** aceito, com pendências

## Contexto

O repositório tinha só as nove labels padrão do GitHub, e **nenhuma das 11 issues
usava qualquer uma delas**. O vocabulário começava do zero.

Ao mesmo tempo, três automações precisavam de labels para funcionar: o formulário
que aplica o tipo, o workflow que cria a branch e o relatório semanal que agrupa
por meta.

## Decisão

Organizar as labels em **quatro eixos independentes**, e não numa lista plana:

| Eixo | Labels | Quem aplica | Pergunta que responde |
|---|---|---|---|
| tipo | `Ingestão`, `DBT`, `Discovery`, `Infra`, *`A classificar`* | o formulário | que trabalho é isso |
| meta | *`meta-2`*, *`meta-3`*, *`meta-5`* | pessoa | para que serve |
| controle | `Código` | pessoa | começar agora |
| natureza | `bug`, `documentation` | pessoa | atravessa o tipo |

*Em itálico: ainda não criadas.*

E: **formulário e label não são um-para-um.** `cruzamento.yml` e `modelo-dbt.yml`
aplicam a mesma label `DBT`. O formulário existe para perguntar certo; a label,
para filtrar.

## Por quê

Label serve a trabalhos diferentes, e misturar tudo num conjunto só é o que faz
vocabulário apodrecer: o menu cresce, ninguém acha a certa, e o conjunto vira
enfeite. Separados por eixo, cada grupo tem regra própria de quem aplica e quando.

O eixo de tipo é consequência de uma escolha já feita no formulário, então
automatizar é de graça e nunca fica errado. O eixo de meta depende de julgamento
que só uma pessoa tem: a issue #2 é uma DAG pelo título, mas existe por causa das
cotas — Meta 3 — e nada no GitHub registra isso hoje.

`bug` e `documentation` atravessam o tipo em vez de substituí-lo: uma DAG quebrada
é `Ingestão` **e** `bug`. Tratá-los como tipo forçaria uma escolha falsa.

## Por que `Código` é manual

É a única label lida por máquina — o `issue-para-branch.yml` compara essa string
e cria a branch. Se um formulário a aplicasse sozinho, toda issue registrada
criaria uma branch na hora, inclusive as que ninguém vai tocar por meses. Manual,
ela separa "registrei" de "vou começar agora".

A branch criada permanece vinculada e a label fica na issue, sinalizando trabalho
em curso. Não há recriação: o gatilho é o evento de rotular, e o workflow confere
se já existe branch vinculada antes de criar.

## Consequências

**O nome de `Código` virou contrato.** Renomear a label pela interface do GitHub
sem mudar o `if` do workflow quebra a automação em silêncio — sem erro, sem
execução falhada, sem aviso. Está documentado em `.github/GUIA.md` e no
cabeçalho do próprio workflow.

**Quatro labels ainda faltam**, e duas funcionalidades ficam pela metade até lá:
`demanda-geral.yml` abre issue sem label enquanto `A classificar` não existir, e
a `revisao-semanal` não agrupa por meta enquanto `meta-2`, `meta-3` e `meta-5`
não existirem. Nos dois casos a degradação é explícita: o formulário tem a linha
comentada com instrução, e a skill registra no relatório que o agrupamento não
foi possível em vez de adivinhar a meta pelo título.

**As sete labels padrão sem uso continuam no repositório** (`duplicate`,
`enhancement`, `good first issue`, `help wanted`, `invalid`, `question`,
`wontfix`). Apagá-las está previsto e ainda não foi feito.
