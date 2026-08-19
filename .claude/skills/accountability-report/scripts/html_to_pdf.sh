#!/usr/bin/env bash
# Converte um HTML em PDF A4, detectando a ferramenta disponível no ambiente.
# Ordem de preferência: Chrome/Chromium headless (melhor suporte a CSS) ->
# weasyprint -> wkhtmltopdf. Uso:
#   bash html_to_pdf.sh <entrada.html> <saida.pdf>
set -euo pipefail

IN="${1:?Uso: html_to_pdf.sh <entrada.html> <saida.pdf>}"
OUT="${2:?Uso: html_to_pdf.sh <entrada.html> <saida.pdf>}"

# Caminho absoluto para o file:// do Chrome
ABS_IN="$(cd "$(dirname "$IN")" && pwd)/$(basename "$IN")"

find_chrome() {
  for c in google-chrome google-chrome-stable chromium chromium-browser chrome msedge; do
    if command -v "$c" >/dev/null 2>&1; then echo "$c"; return 0; fi
  done
  return 1
}

PROFILE="$(mktemp -d)"
trap 'rm -rf "$PROFILE"' EXIT

if CHROME="$(find_chrome)"; then
  echo "Gerando PDF com $CHROME (headless)..."
  "$CHROME" --headless=new --no-sandbox --disable-gpu --no-pdf-header-footer \
    --print-to-pdf="$OUT" "file://$ABS_IN" \
    --user-data-dir="$PROFILE" >/dev/null 2>&1
elif command -v weasyprint >/dev/null 2>&1; then
  echo "Gerando PDF com weasyprint..."
  weasyprint "$IN" "$OUT"
elif command -v wkhtmltopdf >/dev/null 2>&1; then
  echo "Gerando PDF com wkhtmltopdf..."
  wkhtmltopdf --page-size A4 "$IN" "$OUT"
else
  echo "ERRO: nenhuma ferramenta de conversão encontrada." >&2
  echo "Instale uma destas: Google Chrome/Chromium, weasyprint ou wkhtmltopdf." >&2
  echo "O relatório em HTML já está pronto e pode ser impresso como PDF pelo navegador." >&2
  exit 2
fi

if [ -f "$OUT" ]; then
  echo "PDF gerado: $OUT"
  command -v file >/dev/null 2>&1 && file "$OUT" || true
else
  echo "ERRO: o PDF não foi gerado." >&2
  exit 1
fi
