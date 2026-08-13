#!/usr/bin/env bash
#
# Apura os fatos da semana. Só coleta — não interpreta, não resume, não conta
# história. É de propósito: os números do relatório precisam sair daqui, e não
# da leitura do modelo, senão o relatório erra e ninguém percebe.
#
# Uso:  ./apurar.sh [dias]        (padrão: 7)
#
set -uo pipefail

DIAS="${1:-7}"

# `date -d` é GNU, `date -v` é BSD (macOS). O runner do GitHub é Linux, a máquina
# de quem roda à mão costuma ser mac — precisa dos dois.
if date -d "${DIAS} days ago" +%Y-%m-%d >/dev/null 2>&1; then
  DESDE=$(date -d "${DIAS} days ago" +%Y-%m-%d)
else
  DESDE=$(date -v-"${DIAS}"d +%Y-%m-%d)
fi
HOJE=$(date +%Y-%m-%d)

REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner 2>/dev/null || echo "?")

secao() { printf '\n═══ %s ═══\n' "$1"; }
vazio() { printf '  (nenhum)\n'; }

printf 'APURAÇÃO DA SEMANA — dados brutos\n'
printf 'Repositório: %s\n' "$REPO"
printf 'Período: %s a %s (%s dias)\n' "$DESDE" "$HOJE" "$DIAS"
printf 'Gerado em: %s\n' "$(date +'%Y-%m-%d %H:%M')"

# Só `origin` — `--all` percorre todo remote configurado e chegou a levar dois
# minutos em clone com fork adicional, sem trazer nada a mais para este relatório.
git fetch origin --prune --quiet 2>/dev/null || \
  printf '\n[AVISO] git fetch falhou — os dados de branch podem estar desatualizados.\n'

# ─────────────────────────────────────────────────────────────────────────────
secao "COMMITS NA MAIN"
COMMITS_MAIN=$(git log origin/main --since="$DESDE" \
  --pretty=format:'%h|%an|%ad|%s' --date=short 2>/dev/null)
if [ -n "$COMMITS_MAIN" ]; then
  printf '%s\n' "$COMMITS_MAIN" | while IFS='|' read -r h a d s; do
    printf '  %s  %s  %-18s %s\n' "$h" "$d" "${a:0:18}" "$s"
  done
  printf '\n  Total: %s commits\n' "$(printf '%s\n' "$COMMITS_MAIN" | wc -l | tr -d ' ')"
  printf '  Arquivos mais tocados:\n'
  git log origin/main --since="$DESDE" --name-only --pretty=format: 2>/dev/null \
    | grep -v '^$' | sort | uniq -c | sort -rn | head -12 \
    | while read -r n f; do printf '    %3sx  %s\n' "$n" "$f"; done
else
  vazio
fi

# ─────────────────────────────────────────────────────────────────────────────
secao "COMMITS EM OUTRAS BRANCHES (ainda fora da main)"
ACHOU_BRANCH=0
for ref in $(git for-each-ref --format='%(refname:short)' refs/remotes/origin 2>/dev/null); do
  [ "$ref" = "origin/main" ] && continue
  [ "$ref" = "origin/HEAD" ] && continue
  N_TOTAL=$(git rev-list --count origin/main.."$ref" 2>/dev/null || echo 0)
  [ "$N_TOTAL" = "0" ] && continue
  N_SEMANA=$(git rev-list --count --since="$DESDE" origin/main.."$ref" 2>/dev/null || echo 0)
  ULTIMO=$(git log -1 --format='%ad por %an' --date=short "$ref" 2>/dev/null)
  ACHOU_BRANCH=1
  printf '\n  %s\n' "${ref#origin/}"
  printf '    %s commit(s) fora da main, %s nesta semana · último: %s\n' \
    "$N_TOTAL" "$N_SEMANA" "$ULTIMO"
  if [ "$N_SEMANA" != "0" ]; then
    git log --since="$DESDE" --pretty=format:'      %h %ad %s' --date=short \
      origin/main.."$ref" 2>/dev/null
    printf '\n'
  fi
done
[ "$ACHOU_BRANCH" = "0" ] && vazio

# ─────────────────────────────────────────────────────────────────────────────
secao "PULL REQUESTS MERGEADOS NA SEMANA"
gh pr list --state merged --limit 50 \
  --search "merged:>=$DESDE" \
  --json number,title,author,mergedAt,additions,deletions \
  --jq '.[] | "  #\(.number)  \(.mergedAt[:10])  \(.author.login)  +\(.additions)/-\(.deletions)  \(.title)"' \
  2>/dev/null | grep . || vazio

secao "PULL REQUESTS ABERTOS"
gh pr list --state open --limit 50 \
  --json number,title,author,createdAt,isDraft,reviewDecision,updatedAt \
  --jq '.[] | "  #\(.number)  aberto em \(.createdAt[:10])  por \(.author.login)  review=\(if (.reviewDecision // "") == "" then "NENHUM" else .reviewDecision end)  draft=\(.isDraft)  última atividade \(.updatedAt[:10])\n      \(.title)"' \
  2>/dev/null | grep . || vazio

# ─────────────────────────────────────────────────────────────────────────────
secao "ISSUES FECHADAS NA SEMANA"
gh issue list --state closed --limit 50 \
  --search "closed:>=$DESDE" \
  --json number,title,labels,closedAt,assignees \
  --jq '.[] | "  #\(.number)  \(.closedAt[:10])  [\(.labels | map(.name) | join(", "))]  \(.title)"' \
  2>/dev/null | grep . || vazio

secao "ISSUES ABERTAS"
gh issue list --state open --limit 100 \
  --json number,title,labels,createdAt,updatedAt,assignees,comments \
  --jq '.[] | "  #\(.number)  aberta em \(.createdAt[:10])  última atividade \(.updatedAt[:10])  [\(.labels | map(.name) | join(", "))]  \(.comments | length) comentário(s)\n      \(.title)"' \
  2>/dev/null | grep . || vazio

# ─────────────────────────────────────────────────────────────────────────────
secao "VOCABULÁRIO DE LABELS EM USO"
gh label list --limit 60 --json name --jq '[.[].name] | join(", ")' 2>/dev/null \
  | sed 's/^/  /'
printf '\n  [nota] Se não houver labels meta-*, o agrupamento por meta não é possível\n'
printf '         nesta rodada — diga isso no relatório em vez de agrupar por palpite.\n'

secao "FIM DA APURAÇÃO"
printf 'Tudo acima saiu de git e da API do GitHub. Nenhum número foi estimado.\n'
