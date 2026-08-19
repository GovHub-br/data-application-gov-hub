# `.github/`

O que automatiza o fluxo de trabalho do repositório: como a issue nasce, como
vira branch, como o PR é preenchido e o que a CI roda.

## Arquivos

| Caminho | O que faz |
|---|---|
| `ISSUE_TEMPLATE/*.yml` | Os seis formulários de issue. Cada um aplica sozinho a label de tipo |
| `ISSUE_TEMPLATE/config.yml` | Desliga a caixa de texto em branco |
| `PULL_REQUEST_TEMPLATE.md` | Preenchido automaticamente ao abrir PR |
| `TEMPLATES/COMMIT_TEMPLATE.md` | Guia de mensagem de commit (Conventional Commits) |
| `labels.yml` | Referência versionada do vocabulário de labels. O GitHub não lê este arquivo |
| `workflows/main.yaml` | CI: lint, testes, build e push da imagem |
| `workflows/docs-pages.yaml` | Publica o site de documentação (`docs-pages/`) no GitHub Pages |
| `workflows/issue-para-branch.yml` | Label `Código` na issue → cria a branch vinculada |
| `workflows/aviso-branch-orfa.yml` | Issue fechada com branch sem PR mergeado → comenta |
| `actions/setup-poetry/` | Action reutilizável de setup do Poetry |

## Os seis formulários

Vieram dos 14 templates do
[`data-application-cidades`](https://github.com/GovHub-br/data-application-cidades/tree/main/.github/ISSUE_TEMPLATE),
com o texto reaproveitado e o formato trocado — de Markdown legado, sem campo
obrigatório e sem label, para formulário YAML.

A fusão de 14 em 6 seguiu o que as issues do repositório já mostravam: elas nunca
separaram extração de ingestão de DAG.

| Formulário | Funde | Label |
|---|---|---|
| `ingestao-dag.yml` | dag-de-dados, ingestao, extração | `Ingestão` |
| `modelo-dbt.yml` | modelo-dbt, tabela-gold, tratamento | `DBT` |
| `cruzamento.yml` | cruzamento-de-dados | `DBT` |
| `discovery.yml` | requisitos, mapeamento-de-dados | `Discovery` |
| `infra-ci.yml` | (novo — o cidades não tem) | `Infra` |
| `demanda-geral.yml` | demanda-geral-de-dados | *pendente* |

Formulário e label **não são um-para-um**: `cruzamento` e `modelo-dbt` aplicam a
mesma `DBT`. O formulário existe para perguntar certo; a label, para filtrar.

## O que ainda falta

Duas coisas ficaram pendentes de criar labels, e até lá funcionam pela metade:

- **`A classificar`** — sem ela, `demanda-geral.yml` abre a issue sem label
  nenhuma. Depois de criar, descomente a linha `labels:` no formulário.
- **`meta-2`, `meta-3`, `meta-5`** — sem elas, a skill `revisao-semanal` não
  consegue agrupar o relatório por meta, e registra isso em vez de adivinhar.

As três estão especificadas em [`labels.yml`](labels.yml), marcadas com
`status: pendente`.

## Cuidados ao mexer

**O nome da label `Código` é contrato.** O `issue-para-branch.yml` compara essa
string exata. Renomear a label pela interface do GitHub sem mudar o workflow
quebra a automação em silêncio — sem erro, sem aviso, sem execução falhada.

**`Código` é aplicada à mão de propósito.** Se um formulário a aplicasse sozinho,
toda issue registrada criaria uma branch na hora, inclusive as que ninguém vai
tocar por meses.

**`aviso-branch-orfa.yml` nunca apaga nada**, e não deve passar a apagar. Branch
órfã tanto pode ser trabalho abandonado quanto trabalho pronto que ninguém
submeteu, e a diferença não é detectável automaticamente.

**A CI não reprova PR por lint.** O `main.yaml:42` roda `make lint-ci || true`,
por decisão da equipe. É por isso que o checklist do PR diz "rodei `make lint`
localmente" — afirmação de quem abre, não resultado verificado.
