#!/usr/bin/env bash
# Converte o relatório da revisão semanal (Markdown) em HTML e PDF A4.
#
#   ./gerar_pdf.sh <relatorio.md> [saida.pdf]
#
# A conversão em si é a da skill accountability-report — este script não
# reimplementa nada, só a chama com a capa certa e instala o `marked` na
# primeira execução.
#
# No macOS a conversão sai pelo weasyprint, e é de propósito: o Chrome existe,
# mas o binário se chama "Google Chrome" (com espaço), fora do PATH, e não
# aceita ser alcançado por symlink — chamado assim ele aborta, e chamado pelo
# caminho do bundle o `--headless=new --print-to-pdf` trava sem retornar.
# Instale o weasyprint (`pipx install weasyprint`) se ele faltar.
set -euo pipefail

MD="${1:?Uso: gerar_pdf.sh <relatorio.md> [saida.pdf]}"
[ -f "$MD" ] || { echo "ERRO: não achei o markdown: $MD" >&2; exit 1; }

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONVERSOR="$(cd "$AQUI/../.." && pwd)/accountability-report/scripts"

if [ ! -f "$CONVERSOR/build_report.mjs" ] || [ ! -f "$CONVERSOR/html_to_pdf.sh" ]; then
  echo "ERRO: a skill accountability-report não está em .claude/skills/." >&2
  echo "A revisão semanal usa os conversores dela para gerar HTML e PDF." >&2
  echo "Procurado em: $CONVERSOR" >&2
  exit 2
fi

BASE="${MD%.md}"
HTML="$BASE.html"
PDF="${2:-$BASE.pdf}"

# Capa: "Revisão Semanal · <período>", com o período lido do próprio título.
PERIODO="$(sed -n 's/^#[[:space:]]*Revisão da semana[[:space:]]*—[[:space:]]*\(.*\)$/\1/p' "$MD" | head -1)"
TAG="Revisão Semanal"
[ -n "$PERIODO" ] && TAG="Revisão Semanal · $PERIODO"

# marked: instalado uma vez no diretório do conversor, não no projeto.
if [ ! -d "$CONVERSOR/node_modules/marked" ]; then
  echo "Instalando 'marked' (só na primeira execução)..."
  ( cd "$CONVERSOR" \
    && { [ -f package.json ] || npm init -y >/dev/null 2>&1; } \
    && npm install marked@12 >/dev/null 2>&1 ) \
    || { echo "ERRO: falha ao instalar 'marked'. Há npm e rede?" >&2; exit 3; }
fi

node "$CONVERSOR/build_report.mjs" "$MD" "$HTML" "$TAG"

bash "$CONVERSOR/html_to_pdf.sh" "$HTML" "$PDF"

echo
echo "Markdown: $MD"
echo "HTML:     $HTML"
echo "PDF:      $PDF"
