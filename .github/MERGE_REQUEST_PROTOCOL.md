# Protocolo de Aprovação de Pull Requests

Este documento define o fluxo obrigatório para abertura, revisão e aprovação de Pull Requests (PRs) no repositório. O objetivo é garantir qualidade, rastreabilidade e consistência nas contribuições de código, dados, infraestrutura e documentação.

> Neste repositório, o fluxo operacional usa Pull Requests do GitHub. Quando houver referência a Merge Request (MR), considere o mesmo protocolo.

---

## Escopo

Este protocolo se aplica a qualquer PR que envolva:

- Código: novas DAGs, alterações em DAGs existentes, modelos dbt, plugins e helpers
- Documentação: criação ou edição de arquivos `.md`, `schema.yml`, `.github/CONTRIBUTING.md`, entre outros
- Infraestrutura e CI/CD: workflows, configurações, dependências e automações

---

## Fluxo Geral

```text
branch de trabalho -> commits padronizados -> PR aberto -> revisão -> aprovação -> merge na main
```

Nenhum merge deve ser feito diretamente na `main` sem passar pelo fluxo de PR.

---

## 1. Mensagens de Commit

O projeto adota o padrão **Conventional Commits**. Toda mensagem de commit deve seguir a estrutura:

```text
<tipo>[escopo opcional]: <descrição>
```

| Tipo | Quando usar |
| --- | --- |
| `feat` | Nova DAG, novo modelo, nova funcionalidade |
| `fix` | Correção de bug ou comportamento incorreto |
| `docs` | Criação ou edição de documentação |
| `refactor` | Refatoração sem mudança de comportamento |
| `perf` | Melhoria de desempenho |
| `test` | Adição ou correção de testes |
| `build` | Mudanças em dependências ou sistema de build |
| `ci` | Mudanças em configurações de CI |
| `chore` | Ajustes que não afetam código-fonte ou testes |
| `style` | Formatação que não afeta lógica |

Exemplos:

- `feat(siafi): adiciona dag de ingestão de notas de crédito`
- `fix(dbt): corrige deduplicação no modelo slv_contratos_empenhos`
- `docs: adiciona guia de padrões de engenharia`

Regras:

- A descrição deve estar em letras minúsculas e sem ponto final
- O corpo do commit, quando existir, deve ser separado da descrição por uma linha em branco
- Para fechar uma issue automaticamente, use `Closes: #<número>` no rodapé

Consulte também o [template de commit](TEMPLATES/COMMIT_TEMPLATE.md).

---

## 2. Nomenclatura de Branches

O repositório usa dois formatos aceitos.

Formato padrão:

```text
<tipo>/<descricao-curta>
```

Formato com issue vinculada:

```text
<numero-da-issue>-<tipo>-<descricao-curta>
```

Exemplos:

- `feat/siafi-nota-credito-ingestao`
- `fix/fechamento-conn-postgres`
- `docs/protocolo-mr`
- `149-feat-ingestao-sisbolsas`
- `24-fix-dag-nota-de-credito`

---

## 3. Antes de Abrir o PR

Certifique-se de que sua branch está atualizada em relação à `main` do repositório principal:

```bash
git fetch upstream
git rebase upstream/main
```

Se o clone usa apenas o remote `origin` apontando para `GovHub-br/data-application-gov-hub`, use:

```bash
git fetch origin
git rebase origin/main
```

Para PRs de código, execute testes e lint localmente antes de abrir:

```bash
make lint
make test
```

Para PRs de DAGs, rode a DAG localmente e confirme que não há erros de importação:

```bash
airflow dags test <nome_da_dag> <data_execucao>
```

Para PRs de modelos dbt, execute os comandos dentro do projeto dbt alterado:

```bash
cd airflow_lappis/dags/dbt/<projeto>
dbt run --select <modelo>
dbt test --select <modelo>
```

Para PRs de documentação, revise ortografia, links e formatação antes de abrir.

---

## 4. Preenchimento do PR

O título do PR deve ser curto, descritivo e seguir o mesmo padrão dos commits.

A descrição deve conter:

- **O que foi feito:** resumo claro das mudanças
- **Por que foi feito:** contexto ou issue relacionada
- **Como testar:** passos para o revisor validar as mudanças
- **Evidências:** logs, prints, resultados de testes, consultas ou links relevantes
- **Checklist:** itens obrigatórios verificados antes da revisão

Exemplo:

```md
## Descrição

Adicionada DAG de ingestão de notas de crédito do SIAFI.

## Issues relacionadas

Closes #42

## Como testar / validar

1. Subir o ambiente local
2. Triggerar manualmente a DAG `siafi_nota_credito_ingestao`
3. Verificar registros inseridos no schema `siafi`

## Evidências

- `make lint` executado com sucesso
- `make test` executado com sucesso

## Checklist

- [x] Título do PR segue Conventional Commits
- [x] Issue relacionada foi referenciada
- [x] Testes/lint foram executados ou a ausência foi justificada
- [x] Documentação atualizada, se aplicável
```

---

## 5. Revisores e Roteamento por Domínio

### Times de Revisão

O repositório usa times do GitHub para direcionar revisões por domínio de atuação. Os times configurados na organização são:

| Time | Escopo principal |
| --- | --- |
| `@GovHub-br/ipea` | Pipelines, modelos dbt e integrações vinculadas ao IPEA |
| `@GovHub-br/mir` | Pipelines, modelos dbt e integrações vinculadas ao MIR |
| `@GovHub-br/mcid` | Pipelines e integrações vinculadas ao MCid |
| `@GovHub-br/minc` | Pipelines, modelos dbt e integrações vinculadas ao MinC, quando houver paths correspondentes neste repositório |
| `@GovHub-br/oss` | Contribuições externas, documentação pública, governança de colaboração open source e PRs originados na disciplina GCES |

Os nomes acima usam os slugs dos times no GitHub. O time pode aparecer visualmente como `MCid`, `MinC` ou `OSS`, mas automações e revisões devem usar o slug configurado na organização. Não há um time separado para GCES; a label `team:gces` existe para triagem e também solicita revisão do time `@GovHub-br/oss`.

### Estratégia de Revisão Automática

A estratégia adotada para domínios de dados é **GitHub Actions + labels `team:*`**:

- A label `team:*` deve ser adicionada manualmente ao PR conforme o domínio principal da mudança.
- A workflow `Request team review` roda quando uma label é adicionada ao PR.
- A workflow não cria nem aplica labels automaticamente; ela apenas solicita revisão dos times correspondentes às labels já presentes no PR.

Essa escolha evita manter listas frágeis de caminhos na automação, reduz falsos positivos e deixa a decisão do domínio explícita no PR.

Mapa atual dos principais caminhos de DAGs e modelos:

| Caminho | Domínio |
| --- | --- |
| `airflow_lappis/dags/dashboards/` | IPEA |
| `airflow_lappis/dags/dbt/ipea/` | IPEA |
| `airflow_lappis/dags/data_ingest/compras_gov/` | IPEA, exceto subpasta `mir/` |
| `airflow_lappis/dags/data_ingest/ipea_pro/` | IPEA |
| `airflow_lappis/dags/data_ingest/pncp/` | IPEA |
| `airflow_lappis/dags/data_ingest/sgac/` | IPEA |
| `airflow_lappis/dags/data_ingest/siafi/` | IPEA |
| `airflow_lappis/dags/data_ingest/siape/` | IPEA |
| `airflow_lappis/dags/data_ingest/siorg/` | IPEA |
| `airflow_lappis/dags/data_ingest/sisbolsas/` | IPEA |
| `airflow_lappis/dags/data_ingest/tesouro_gerencial/` | IPEA, exceto subpastas `mir/` e `mcid/` |
| `airflow_lappis/dags/data_ingest/transfere_gov/` | IPEA, exceto subpasta `mir/` |
| `airflow_lappis/dags/dbt/mir/` | MIR |
| `airflow_lappis/dags/data_ingest/compras_gov/mir/` | MIR |
| `airflow_lappis/dags/data_ingest/dados_abertos/` | MIR |
| `airflow_lappis/dags/data_ingest/siconv/` | MIR |
| `airflow_lappis/dags/data_ingest/tesouro_gerencial/mir/` | MIR |
| `airflow_lappis/dags/data_ingest/transfere_gov/mir/` | MIR |
| `airflow_lappis/dags/data_ingest/transferegov_emendas/` | MIR |
| `airflow_lappis/dags/data_ingest/tesouro_gerencial/mcid/` | MCid |
| `airflow_lappis/dags/data_ingest/ibge/` | GCES / OSS |

Quando um PR alterar arquivos de um domínio, o autor ou mantenedor deve aplicar a label `team:*` correspondente. A workflow solicita review do time associado a essa label. A ruleset da branch `main` deve exigir pelo menos uma aprovação antes do merge.

### Labels de Apoio

Labels são a referência para o roteamento automático de revisores:

- `team:ipea`
- `team:mir`
- `team:mcid`
- `team:minc`
- `team:gces`
- `team:oss`

Se uma label indicar um domínio diferente dos arquivos alterados, o autor deve justificar no PR ou ajustar a label antes do merge.

### Como Incluir Novos Projetos ou Times

Para adicionar um novo projeto, ministério ou disciplina ao fluxo de revisão:

1. Criar o time correspondente na organização `GovHub-br`.
2. Adicionar os membros responsáveis ao time.
3. Definir o slug oficial do time, por exemplo `@GovHub-br/minc`.
4. Criar a label `team:*` correspondente, se ela ainda não existir.
5. Mapear a label para o slug do time na workflow `Request team review`.
6. Documentar quando a label deve ser usada.
7. Abrir um PR de teste, aplicar a label correspondente e confirmar que o GitHub solicita revisão do time correto.

### Número Mínimo de Aprovações

- PRs de código: mínimo de **1 aprovação** de uma pessoa revisora do domínio alterado
- PRs de documentação: mínimo de **1 aprovação** de uma pessoa responsável pelo tipo de documentação
- PRs críticos, sensíveis ou com impacto em produção: recomendado exigir **2 aprovações**

### Configuração Recomendada no GitHub

Para aplicar essas regras automaticamente, o repositório usa a workflow `Request team review`.

Caso o GitHub recuse a solicitação automática de review de um time, a workflow deve registrar um aviso sem bloquear o PR. Se a solicitação de reviewers por time continuar falhando, configure o secret `TEAM_REVIEW_TOKEN` com um token autorizado a solicitar reviewers no repositório e ler times da organização.

Além da workflow, a branch `main` deve manter as seguintes proteções habilitadas:

- Proteção da branch `main`, exigindo revisão antes do merge
- Mínimo de **1 aprovação** antes do merge
- Bloqueio de force push na branch protegida

Essas configurações de branch dependem da administração do repositório. Enquanto não estiverem ativas, as regras deste protocolo devem ser verificadas manualmente por autores, revisores e mantenedores.

---

## 6. Durante a Revisão

O revisor deve:

- Aprovar o PR se estiver tudo certo
- Solicitar mudanças com comentários claros e objetivos
- Explicar o que está errado e, sempre que possível, sugerir como corrigir
- Bloquear o PR com `request changes` apenas em casos de problema real: bug, credencial exposta, violação de padrão crítico ou dado sensível

Comentários de estilo ou preferência pessoal que não violam nenhum padrão documentado **não devem bloquear** o merge. Eles podem ser deixados como sugestão opcional.

Recomenda-se que a primeira resposta de revisão aconteça em até **2 dias úteis**, salvo indisponibilidade do time.

---

## 7. Após Pedido de Mudança

O autor deve:

- Responder a **todos** os comentários antes de pedir nova revisão
- Aplicar a mudança solicitada ou justificar por que ela não faz sentido
- Marcar cada comentário como resolvido após endereçá-lo
- Avisar o revisor quando as mudanças estiverem prontas para uma nova rodada

Se houver discordância sobre um comentário, a discussão deve acontecer na própria thread do PR. Se não houver consenso, escale para o time no canal de comunicação para evitar que o PR fique parado indefinidamente.

---

## 8. Critérios de Aprovação

### Para Código

- [ ] A DAG ou modelo segue os padrões de engenharia e organização do repositório
- [ ] Não há `SELECT *` em modelos finais dbt
- [ ] Não há credenciais ou dados sensíveis commitados
- [ ] Testes passam (`make test`, `dbt test`)
- [ ] Lint passa (`make lint`)
- [ ] Commits seguem Conventional Commits
- [ ] A lógica está correta e o código é legível
- [ ] Plugins e helpers existentes foram reaproveitados quando aplicável

### Para Documentação

- [ ] O conteúdo é preciso e reflete o estado real do repositório
- [ ] A formatação Markdown está correta
- [ ] Links estão funcionando
- [ ] Não contradiz outras documentações existentes
- [ ] Commits seguem Conventional Commits

---

## 9. Merge

- O merge só pode ser feito após todas as aprovações necessárias
- Usar **merge commit** como padrão do repositório, preservando o histórico completo dos PRs
- Deletar a branch após o merge

---

## 10. PRs Urgentes

Em casos excepcionais que exijam merge imediato, como incidente em produção ou correção crítica:

- Notificar o time no canal de comunicação antes de mergear
- Obter no mínimo **1 aprovação** de pessoa do time responsável
- Abrir issue de acompanhamento para revisão posterior, se necessário
