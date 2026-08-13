---
name: to-issues-minc
description: >-
  Transforma um plano, uma conversa ou uma demanda solta em issues no formato dos
  formulários deste repositório, já com o tipo e a label certos. Use quando o
  usuário pedir "cria as issues disso", "quebra isso em issues", "transforma esse
  plano em tarefas", "abre uma issue para X", ou quando uma discovery terminar e
  as issues de implementação precisarem sair dela.
allowed-tools: Bash, Read, Grep, Glob
---

# Escrever issues no formato do repositório

As issues aqui nascem de formulário, não de caixa em branco — a caixa está
desligada de propósito. Esta skill escreve o corpo já no formato do formulário
certo, para a issue nascer completa mesmo quando criada pela linha de comando.

## Passo 1 — Escolher o formulário

Seis, em [`.github/ISSUE_TEMPLATE/`](../../../.github/ISSUE_TEMPLATE/). Leia o
arquivo antes de escrever: ele é a fonte dos campos, e se mudar, ele vence esta
skill.

| Formulário | Quando | Label | Prefixo do título |
|---|---|---|---|
| `ingestao-dag.yml` | entre a origem e o datalake — cliente de API, DAG, carga | `Ingestão` | `[Ingestão]` |
| `modelo-dbt.yml` | transformação em `dbt/minc/models/`, qualquer camada | `DBT` | `[dbt]` |
| `cruzamento.yml` | junção de duas ou mais bases por uma chave | `DBT` | `[Cruzamento]` |
| `discovery.yml` | ainda não se sabe o bastante para implementar | `Discovery` | `[Discovery]` |
| `infra-ci.yml` | Docker, Airflow, Superset, CI, credencial | `Infra` | `[Infra]` |
| `demanda-geral.yml` | nenhum dos outros serve | — | sem prefixo |

Repare que formulário e label não são um-para-um: `cruzamento` e `modelo-dbt`
aplicam a mesma label `DBT`. O formulário existe para perguntar certo; a label,
para filtrar.

Na dúvida entre dois, escolha o mais específico. Só caia em `demanda-geral`
quando genuinamente não der para classificar — ela existe para registrar o que
ainda precisa de triagem, não para fugir da escolha.

## Passo 2 — Quebrar o trabalho

Prefira **fatias verticais**: cada issue entrega um caminho completo e
verificável sozinho, e não uma camada horizontal de várias coisas.

Neste repositório isso quase sempre significa separar por **fonte** ou por
**modelo**, não por etapa. "Ingerir programas do TransfereGov" é uma fatia
fechada; "criar todos os clientes de API" e depois "criar todas as DAGs" são duas
metades que só valem juntas — e a primeira fica meses sem poder ser validada.

Uma issue por fonte, por modelo ou por pergunta de discovery. Se a issue não cabe
numa frase, provavelmente são duas.

## Passo 3 — Preencher

Escreva o corpo com os mesmos títulos de seção do formulário, na mesma ordem. Os
campos marcados `required:` no `.yml` **não podem ficar vazios** — se a informação
não existe ainda, ou você pergunta ao usuário, ou o tipo certo é `discovery`, e
não implementação.

Campo que você não sabe e que não é obrigatório: escreva `a definir` em vez de
deixar em branco. Em branco parece esquecimento; `a definir` é uma decisão
registrada.

## Passo 4 — Apresentar antes de criar

Mostre a lista numerada ao usuário com título, tipo e dependências entre elas, e
pergunte se a granularidade está boa — grossa demais, fina demais. **Espere
aprovação antes de criar qualquer issue.**

Depois de aprovado:

```bash
gh issue create --title "[dbt] <titulo>" --label "DBT" --body-file <arquivo>
```

A label precisa bater exatamente com o que existe no repositório, acento e
maiúscula incluídos — confira com `gh label list`. Label que não existe é
ignorada em silêncio, e a issue nasce sem tipo.

## Passo 5 — Meta e vínculos

Se a issue serve a uma meta do PNAB — Meta 2 (Execução Financeira), Meta 3
(Diversidade, Cotas e Territórios), Meta 5 (Primeiro Acesso) — diga isso no
corpo. Quando as labels `meta-*` existirem, aplique-as também; enquanto não
existirem, o texto é o único registro do vínculo, e é ele que a
[`revisao-semanal`](../revisao-semanal/SKILL.md) vai conseguir ler.

Se as issues saíram de uma discovery, referencie a issue de origem em cada uma e
comente na origem listando as que nasceram dela.

## Antes de considerar pronto

- [ ] Cada issue usa o formulário mais específico que serve
- [ ] Nenhum campo obrigatório do formulário ficou vazio
- [ ] As fatias são verticais — cada uma dá para validar sozinha
- [ ] A label existe no repositório, escrita exatamente igual
- [ ] O vínculo com a meta está registrado, quando houver
- [ ] O usuário aprovou a lista antes de qualquer `gh issue create`
