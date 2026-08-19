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

# Janela das issues abertas: o dobro do período (14 dias no padrão de 7). Issue
# sem movimento nesse intervalo não entra na apuração. O que interessa aqui é o
# que foi feito no período e o que encostou nele há pouco; backlog antigo é
# outro assunto, e listá-lo toda semana só enchia o relatório de ruído.
DIAS_ISSUES=$((DIAS * 2))
if date -d "${DIAS_ISSUES} days ago" +%Y-%m-%d >/dev/null 2>&1; then
  DESDE_ISSUES=$(date -d "${DIAS_ISSUES} days ago" +%Y-%m-%d)
else
  DESDE_ISSUES=$(date -v-"${DIAS_ISSUES}"d +%Y-%m-%d)
fi

REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner 2>/dev/null || echo "?")

secao() { printf '\n═══ %s ═══\n' "$1"; }
vazio() { printf '  (nenhum)\n'; }

# Roda um comando do `gh` distinguindo três desfechos: veio dado, não veio nada,
# ou a API falhou. A distinção é o ponto. Com `2>/dev/null | grep . || vazio`,
# um 503 do GitHub sai do script como "(nenhum)" — e quem escreve o relatório
# registra "zero PRs mergeados" sem saber que a pergunta nunca foi respondida.
# Num relatório que proíbe estimar, um zero falso é o pior defeito possível.
#
# Tenta três vezes antes de desistir. A API do GitHub devolve 503 esporádico, e
# como são cinco chamadas por execução, uma falha isolada bastava para deixar o
# relatório incompleto. Só a falha persistente vira aviso.
gh_secao() {
  vazio_msg="$1"; shift
  err=$(mktemp)
  rc=0
  for tentativa in 1 2 3; do
    saida=$("$@" 2>"$err"); rc=$?
    [ "$rc" -eq 0 ] && break
    [ "$tentativa" -lt 3 ] && sleep 2
  done
  if [ "$rc" -ne 0 ]; then
    printf '  [FALHA NA API após 3 tentativas — esta seção NÃO foi apurada]\n'
    head -3 "$err" | sed 's/^/    /'
    printf '    >>> não escreva número nem "nenhum" para esta seção no relatório;\n'
    printf '        diga que não foi possível apurar, ou rode de novo. <<<\n'
  elif [ -n "$saida" ]; then
    printf '%s\n' "$saida"
  else
    printf '  %s\n' "$vazio_msg"
  fi
  rm -f "$err"
}

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
# Só branches que receberam commit NO PERÍODO. Uma branch parada não entra —
# aqui o trabalho vai da extração do dado até a visualização, e esse ciclo não
# cabe numa semana: branch sem commit nos últimos dias é o ritmo normal do
# processo, não trabalho travado. Listá-las toda semana produzia alarme falso.
secao "COMMITS EM OUTRAS BRANCHES NO PERÍODO (ainda fora da main)"
ACHOU_BRANCH=0
for ref in $(git for-each-ref --format='%(refname:short)' refs/remotes/origin 2>/dev/null); do
  [ "$ref" = "origin/main" ] && continue
  [ "$ref" = "origin/HEAD" ] && continue
  N_TOTAL=$(git rev-list --count origin/main.."$ref" 2>/dev/null || echo 0)
  [ "$N_TOTAL" = "0" ] && continue
  N_SEMANA=$(git rev-list --count --since="$DESDE" origin/main.."$ref" 2>/dev/null || echo 0)
  [ "$N_SEMANA" = "0" ] && continue
  ULTIMO=$(git log -1 --format='%ad por %an' --date=short "$ref" 2>/dev/null)
  ACHOU_BRANCH=1
  printf '\n  %s\n' "${ref#origin/}"
  printf '    %s commit(s) no período · %s fora da main ao todo · último: %s\n' \
    "$N_SEMANA" "$N_TOTAL" "$ULTIMO"
  git log --since="$DESDE" --pretty=format:'      %h %ad %s' --date=short \
    origin/main.."$ref" 2>/dev/null
  printf '\n'
done
[ "$ACHOU_BRANCH" = "0" ] && printf '  (nenhuma branch recebeu commit no período)\n'

# ─────────────────────────────────────────────────────────────────────────────
secao "PULL REQUESTS MERGEADOS NA SEMANA"
gh_secao '(nenhum)' \
  gh pr list --state merged --limit 50 \
  --search "merged:>=$DESDE" \
  --json number,title,author,mergedAt,additions,deletions \
  --jq '.[] | "  #\(.number)  \(.mergedAt[:10])  \(.author.login)  +\(.additions)/-\(.deletions)  \(.title)"'

secao "PULL REQUESTS ABERTOS"
gh_secao '(nenhum)' \
  gh pr list --state open --limit 50 \
  --json number,title,author,createdAt,isDraft,reviewDecision,updatedAt \
  --jq '.[] | "  #\(.number)  aberto em \(.createdAt[:10])  por \(.author.login)  review=\(if (.reviewDecision // "") == "" then "NENHUM" else .reviewDecision end)  draft=\(.isDraft)  última atividade \(.updatedAt[:10])\n      \(.title)"'

# ─────────────────────────────────────────────────────────────────────────────
secao "ISSUES FECHADAS NA SEMANA"
gh_secao '(nenhum)' \
  gh issue list --state closed --limit 50 \
  --search "closed:>=$DESDE" \
  --json number,title,labels,closedAt,assignees \
  --jq '.[] | "  #\(.number)  \(.closedAt[:10])  [\(.labels | map(.name) | join(", "))]  \(.title)"'

# O recorte da janela é do próprio GitHub (`updated:>=`), como nas duas seções
# acima. Não troque isso por um filtro de data no jq via `--arg`: o `gh` não tem
# essa flag, lê o `--arg` como se fosse o valor de `--jq` e aborta a chamada
# inteira — a seção não sai errada, sai inexistente.
secao "ISSUES ABERTAS COM MOVIMENTO DESDE $DESDE_ISSUES"
gh_secao '(nenhuma issue aberta teve movimento na janela)' \
  gh issue list --state open --limit 100 \
  --search "updated:>=$DESDE_ISSUES sort:updated-desc" \
  --json number,title,labels,createdAt,updatedAt,assignees,comments \
  --jq '.[] | "  #\(.number)  aberta em \(.createdAt[:10])  última atividade \(.updatedAt[:10])  [\(.labels | map(.name) | join(", "))]  \(.comments | length) comentário(s)\n      \(.title)"'

# ─────────────────────────────────────────────────────────────────────────────
secao "VOCABULÁRIO DE LABELS EM USO"
gh_secao '(nenhuma label)' \
  gh label list --limit 60 --json name --jq '"  " + ([.[].name] | join(", "))'
printf '\n  [nota] Se não houver labels meta-*, o agrupamento por meta não é possível\n'
printf '         nesta rodada — diga isso no relatório em vez de agrupar por palpite.\n'

secao "FIM DA APURAÇÃO"
printf 'Tudo acima saiu de git e da API do GitHub. Nenhum número foi estimado.\n'
