---
name: commit-smart
description: Analyze staged/unstaged changes and create semantic conventional commits with context about WHY, not just WHAT. Auto-detects commit type and scope from the diff. Supports optional type/scope arguments. Usage - /commit-smart, /commit-smart fix, /commit-smart refactor api
---

<!-- PROCEDÊNCIA
Copiada de GovHub-br/GovHub-skills, pasta 05-qualidade-testes/commit-smart
Commit de origem: 8ffa097e6c3e5f63c6de73aca9d976897b1307b0 (2026-08-10)

Cópia, e não instalação por marketplace: a equipe recebe as skills ao
clonar, sem rodar nada. O custo é que correção feita lá em cima não chega
sozinha aqui — para atualizar, recopie a pasta e troque o commit acima.
-->

# Smart Commit

Create meaningful conventional commits by analyzing your actual changes.

## Neste repositório

O guia canônico de mensagem de commit é
[`.github/TEMPLATES/COMMIT_TEMPLATE.md`](../../../.github/TEMPLATES/COMMIT_TEMPLATE.md).
Ele manda no que estiver abaixo: a mensagem é em **português**, e o rodapé usa
`Refs: #42` ou `Closes: #78`.

Escopo, pela pasta afetada:

| Caminho | Escopo |
|---|---|
| `dags/data_ingest/<fonte>/` | o nome da fonte — `bbagil`, `territorio`, `transferegov` |
| `dbt/minc/models/<dominio>/` | `cotas`, `agentes` |
| `plugins/cliente_*.py` | o sistema de origem |
| `helpers/` | `helpers` |
| `infra/`, `.github/workflows/` | `infra`, `ci` |
| `.github/ISSUE_TEMPLATE/`, `.claude/` | omita o escopo |

**Não há hook de pre-commit.** A formatação não roda sozinha ao commitar — se o
seu commit toca Python ou SQL, rode `make format` antes, e confira no
`git status` que ele não reformatou arquivo fora do seu escopo (ele age no
repositório inteiro). Reverta o que não é seu com `git restore <caminhos>` antes
de dar `git add`: arquivo reformatado por tabela polui o diff e esconde a sua
mudança na revisão.

O `pre-push` continua ativo e roda `make lint` e `make test`.

## Workflow

### Step 1: Assess the working tree

Run these commands to understand the current state:

```bash
git status
git diff --stat
git diff --cached --stat
```

### Step 2: Handle unstaged changes

If nothing is staged (`git diff --cached` is empty):

1. Show the user what files have changed
2. Suggest what to stage based on logical grouping (e.g., "these 3 files are all related to the auth refactor")
3. Ask if they want to stage all, or select specific files
4. Stage the approved files with `git add <files>`

If changes are already staged, proceed to analysis.

### Step 3: Analyze the diff

Read the full staged diff:

```bash
git diff --cached
```

Determine the commit type from the changes:

| Signal | Type |
|--------|------|
| New files with new functionality | `feat` |
| New test files or test additions | `test` |
| Changes to existing logic fixing incorrect behavior | `fix` |
| Structural changes without behavior change | `refactor` |
| package.json, tsconfig, CI config changes | `chore` |
| Build/bundler config changes | `build` |
| README, docs, comments only | `docs` |
| Formatting, whitespace, semicolons only | `style` |
| Performance improvements | `perf` |

Determine the scope from the primary directory or module affected:
- `src/api/` -> `api`
- `src/components/auth/` -> `auth`
- `tests/` -> `tests`
- Root config files -> omit scope
- Multiple unrelated areas -> omit scope

### Step 4: Check for user overrides

If the user provided arguments via `$ARGUMENTS`:
- Single word (e.g., `fix`) -> use as commit type
- Two words (e.g., `refactor api`) -> use as type and scope
- Otherwise -> use auto-detected values

### Step 5: Compose the commit message

Format: `type(scope): imperative short description`

Rules:
- Subject line max 72 characters
- Use imperative mood ("add", "fix", "refactor", not "added", "fixes")
- Don't end with a period
- Body explains **WHY** this change was made, not what changed (the diff shows what)
- If changes are trivial (typo fix, formatting), skip the body

Example:
```
feat(auth): add JWT refresh token rotation

Tokens were expiring mid-session for users with slow connections.
Rotating refresh tokens extends the session without compromising
security, since each refresh token can only be used once.
```

### Step 6: Confirm and commit

Show the user the proposed commit message and ask for confirmation.

If confirmed, run:
```bash
git commit -m "<message>"
```

Then verify with:
```bash
git log --oneline -1
```

Show the committed hash and message.

## Tips

- Run after completing a logical unit of work, not after every file change
- If the diff is too large for one commit, suggest splitting into multiple commits
- For breaking changes, add `!` after the scope: `feat(api)!: change response format`
- The body should answer "if someone reads this commit in 6 months, will they understand WHY?"
