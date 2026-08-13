---
name: abrir-pr
description: >-
  Escreve o pull request deste repositório a partir dos commits da branch —
  título em Conventional Commits, descrição, issue vinculada, impacto em dados e
  checklist, no formato de .github/PULL_REQUEST_TEMPLATE.md. Use quando o usuário
  pedir "abre o PR", "cria o pull request", "escreve a descrição do PR", "prepara
  o PR", ou disser que terminou o trabalho da branch e quer submeter.
allowed-tools: Bash, Read, Grep, Glob
---

# Abrir o pull request

Preenche o template do repositório a partir do que a branch realmente fez. O
template está em [`.github/PULL_REQUEST_TEMPLATE.md`](../../../.github/PULL_REQUEST_TEMPLATE.md)
e manda no formato — se ele mudar, ele vence esta skill.

## Passo 1 — Ler a branch

```bash
git fetch origin --quiet
git log origin/main..HEAD --pretty=format:'%h %s' --date=short
git diff origin/main...HEAD --stat
```

Se a saída vier vazia, a branch não tem nada a submeter — diga isso e pare.

Descubra a issue vinculada, nesta ordem:

1. O número no começo do nome da branch (`24-fix-dag-nota-de-credito` → #24).
2. Um `Refs:` ou `Closes:` no corpo dos commits.
3. Se não achar, **pergunte**. PR sem issue neste repositório é exceção, não regra.

## Passo 2 — Entender a mudança

Leia o diff de verdade, não só o `--stat`. O que a descrição precisa dizer é o
que mudou de **comportamento** — não quais arquivos foram tocados, que o próprio
GitHub já mostra.

Preste atenção especial em três coisas, porque são as que o revisor não vê no
diff e as que dão problema depois:

- **Mudou schema, tabela ou coluna?** Vai na seção de impacto em dados.
- **Algum modelo dbt ou DAG precisa rodar de novo depois do merge?** Em que ordem.
- **Algum número que já está em uso muda?** Dashboard, relatório de meta,
  resposta já entregue a alguém. Se muda, a contagem antes e depois é obrigatória
  nas evidências.

## Passo 3 — Título

Conventional Commits, em português, como o resto do repositório:

```text
<tipo>(<escopo opcional>): <descrição em minúscula, sem ponto final>
```

O tipo sai do conjunto da mudança, não do commit mais recente: uma branch com
três `feat` e um `fix` é `feat`. O guia completo de tipos está em
[`.github/TEMPLATES/COMMIT_TEMPLATE.md`](../../../.github/TEMPLATES/COMMIT_TEMPLATE.md).

## Passo 4 — Preencher

Preencha todas as seções do template. Duas regras sobre o checklist:

- **Só marque o que você verificou.** O item de lint diz "rodei `make lint`
  localmente" porque a CI roda `lint-ci` com `|| true` e não reprova o PR — marcar
  sem ter rodado é afirmar o que ninguém checou. Se não rodou, rode; se não deu
  para rodar, deixe desmarcado e explique na descrição.
- **Item que não se aplica fica desmarcado, com a razão na descrição.** Não
  invente aplicabilidade para deixar o checklist bonito.

Na seção de impacto em dados, "nenhum" é resposta legítima quando o PR não toca
DAG nem modelo — escreva "nenhum" e siga, não apague a seção.

## Passo 5 — Abrir

Mostre o texto completo ao usuário e **espere aprovação** antes de criar. Abrir
PR é ação visível para o time inteiro.

```bash
gh pr create --title "<titulo>" --body-file <arquivo> --base main
```

Se já existir PR para a branch, atualize em vez de criar outro:

```bash
gh pr edit <numero> --title "<titulo>" --body-file <arquivo>
```

## Antes de considerar pronto

- [ ] O título descreve a branch inteira, não o último commit
- [ ] A issue está vinculada com `Closes #`
- [ ] A descrição diz o que mudou de comportamento, não que arquivos mudaram
- [ ] O impacto em dados está preenchido, nem que seja com "nenhum"
- [ ] Se algum número em uso muda, a contagem antes e depois está nas evidências
- [ ] Todo item marcado no checklist foi de fato verificado
- [ ] Nenhuma credencial, CPF, CNPJ ou dado pessoal no texto ou nos prints
