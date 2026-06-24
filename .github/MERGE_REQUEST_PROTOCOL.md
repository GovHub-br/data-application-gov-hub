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

## 5. Revisores

### Quem Deve Revisar

| Tipo de PR | Revisores |
| --- | --- |
| DAGs de ingestão | `@GovHub-br/developers` |
| Modelos dbt | `@GovHub-br/developers` |
| Plugins e helpers | `@GovHub-br/developers` |
| Documentação de configuração, infraestrutura ou deploy | `@GovHub-br/infra` |
| Documentação de DAGs, modelos dbt ou fluxo de dados | `@GovHub-br/developers` |

Os times `@GovHub-br/developers` e `@GovHub-br/infra` são os responsáveis atuais pelos domínios técnicos do repositório. Caso um PR altere mais de um domínio, solicite revisão de todos os times responsáveis pelas áreas impactadas.

### Número Mínimo de Aprovações

- PRs de código: mínimo de **1 aprovação** de uma pessoa revisora do domínio alterado
- PRs de documentação: mínimo de **1 aprovação** de uma pessoa responsável pelo tipo de documentação
- PRs críticos, sensíveis ou com impacto em produção: recomendado exigir **2 aprovações**

### Configuração Recomendada no GitHub

Para aplicar essas regras automaticamente, o repositório usa um arquivo [`CODEOWNERS`](CODEOWNERS) com os times responsáveis por cada domínio técnico.

Além do `CODEOWNERS`, a branch `main` deve manter as seguintes proteções habilitadas:

- Proteção da branch `main`, exigindo revisão antes do merge
- Opção **Require review from Code Owners** habilitada

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
