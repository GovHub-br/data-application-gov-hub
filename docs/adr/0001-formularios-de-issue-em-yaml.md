# ADR 0001 — Formulários de issue em YAML, com o texto do cidades

- **Data:** 2026-08-13
- **Status:** aceito

## Contexto

As issues do repositório nasciam de caixa de texto em branco. As 11 existentes
não tinham label nenhuma, e o tipo de cada uma só era legível por um prefixo
informal no título — `[dbt]`, `[Exploração]`, `[Discovery/LPG]`, `[Refactor/PNAB]`.

O `data-application-cidades` já tinha 14 templates de issue, com texto bom e
pensado para trabalho de dados. Mas estão no formato Markdown legado: o corpo é
uma sugestão que pode ser apagada inteira, nenhum campo é obrigatório, e o campo
`labels` está vazio nos 14 arquivos.

## Decisão

Reaproveitar o **texto** do cidades e trocar o **formato**, escrevendo formulários
YAML (`issue forms`) com `validations: required` nos campos que causam ida e
volta, e `labels:` preenchido.

Fundir os 14 tipos em 6, por fusão e não por corte: cada bloco específico dos
originais é preservado dentro do formulário correspondente.

| Formulário | Funde |
|---|---|
| `ingestao-dag` | dag-de-dados, ingestao, extração |
| `modelo-dbt` | modelo-dbt, tabela-gold, tratamento |
| `cruzamento` | cruzamento-de-dados |
| `discovery` | requisitos, mapeamento-de-dados |
| `infra-ci` | novo |
| `demanda-geral` | demanda-geral-de-dados |

## Por quê

A evidência veio das próprias issues do repositório. As #2, #3 e #4 são
literalmente "extração", "ingestão" e "DAG" no vocabulário do cidades — e nunca
foram separadas. A #2 ("Extração de Anexos em Base64 e Carga no Datalake") é as
três coisas na mesma issue. Manter três portas para isso pede uma escolha que
ninguém faz, e o resultado previsível é todo mundo cair em "demanda geral".

Além disso, `dag-de-dados`, `ingestao` e `extração` compartilham Contexto,
Objetivo, Fonte e Destino palavra por palavra. Divergem em um bloco cada. A união
é um formulário com os quatro comuns mais os três específicos, sem perder linha.

Os 14 também são uma taxonomia de *entrada de demanda*, não de trabalho:
`requisitos`, `pacote-de-entrega`, `relatorio` e `mapeamento-politico` existem
porque o cidades recebe pedidos do MCid. Aqui as issues são tarefa de engenharia
contra meta.

## Alternativas descartadas

**Copiar os 14 templates como estão.** Dá paridade com o repositório irmão e
nenhuma decisão de fusão a defender. Descartada porque não entrega o que a
proposta pedia: sem campo obrigatório e sem label, a issue continua nascendo
incompleta — só que com mais passos.

**Manter os 14, convertidos para YAML.** Resolveria o campo obrigatório, mas
mantém o menu longo, e com ele a distinção extração/ingestão/tratamento que a
prática do repositório mostra não se sustentar.

## Consequências

Perde-se um eixo de filtro: com 14 labels dava para filtrar "só tabela gold";
com 6, `gold` é um dropdown `camada` dentro do formulário de dbt — fica no corpo,
não filtra na lista. Se esse filtro fizer falta, recupera-se com um workflow que
lê o dropdown e aplica a label. Não foi feito agora porque ninguém pediu.

Tabelas Markdown não são entrada de formulário: as de "campos principais" viraram
`textarea` com a tabela pré-preenchida em `value:`, o que dá o mesmo resultado.
