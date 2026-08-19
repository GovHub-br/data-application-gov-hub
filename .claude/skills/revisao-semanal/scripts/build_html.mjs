// Converte a revisão semanal (Markdown GFM) em HTML autocontido, na identidade
// visual do GovHub. Uso:
//   node build_html.mjs <entrada.md> <saida.html> ["Texto do kicker"]
//
// Por que este arquivo existe, se a accountability-report já converte Markdown:
// a conversão é a mesma, a identidade não. Aquele relatório é azul, de entrega
// oficial a órgão; este é um documento de acompanhamento interno do GovHub, e a
// marca é o roxo #7A34F3. Como a accountability-report é cópia versionada do
// GovHub-skills (editar lá se perde na próxima recópia), o tema vive aqui e só
// o passo HTML→PDF continua sendo o dela.
//
// Tokens: GovHub-skills/01-govhub/govhub-visual-identity/references/tokens.css
import { readFileSync, writeFileSync } from 'fs';
import { fileURLToPath } from 'url';

const AQUI = new URL('.', import.meta.url);

// `marked` fica instalado junto do conversor da accountability-report — uma
// instalação só para as duas skills. O gerar_pdf.sh garante que exista.
const MARKED = new URL('../../accountability-report/scripts/node_modules/marked/lib/marked.esm.js', AQUI);
let marked;
try {
  ({ marked } = await import(MARKED));
} catch {
  console.error("ERRO: não achei o pacote 'marked'.");
  console.error('Esperado em: ' + fileURLToPath(MARKED));
  console.error('Rode a geração pelo gerar_pdf.sh, que o instala na primeira vez.');
  process.exit(3);
}

const [, , mdPath, outPath, kickerArg] = process.argv;
if (!mdPath || !outPath) {
  console.error('Uso: node build_html.mjs <entrada.md> <saida.html> ["kicker"]');
  process.exit(1);
}

const md = readFileSync(mdPath, 'utf8');
const body = marked.parse(md, { gfm: true, breaks: false });

const h1 = (md.match(/^#\s+(.+)$/m) || [])[1] || 'Revisão semanal';
const kicker = kickerArg || 'Revisão Semanal';

// O corpo já vem como HTML do `marked`, mas o título e o kicker entram crus no
// documento. Um "&" ou um "<" no título do relatório — o nome de uma branch, um
// intervalo escrito com sinal — sairia como marcação e quebraria a página.
const esc = (s) =>
  String(s).replace(/[&<>"]/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);

// Logo oficial embutida como data URI: o HTML precisa abrir sozinho, e o
// weasyprint não busca arquivo externo de forma confiável.
let logoTag = '';
try {
  const svg = readFileSync(fileURLToPath(new URL('../assets/govhub-horizontal-light.svg', AQUI)));
  logoTag = `<img class="gh-logo" alt="Gov Hub" src="data:image/svg+xml;base64,${svg.toString('base64')}">`;
} catch {
  logoTag = '';  // sem logo o documento continua válido, só menos marcado
}

const css = `
:root {
  --primary-purple: #7A34F3;
  --logo-purple:    #7521F9;
  --purple-600:     #7C3AAD;
  --purple-700:     #5B21B6;
  --accent-orange:  #F97316;
  --text-strong:    #202020;
  --text-body:      #2D3748;
  --text-muted:     #666666;
  --bg-white:       #FFFFFF;
  --bg-subtle:      #F8F9FA;
  --linha:          #E6E2F0;
  --linha-forte:    #CFC9DE;
}
* { box-sizing: border-box; }

/* Inter quando instalada; sem @import, porque a geração do PDF roda offline
   e uma webfont pendurada na rede trava ou entrega fallback silencioso. */
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
  color: var(--text-body);
  line-height: 1.6;
  font-size: 10.5pt;
  margin: 0;
  background: var(--bg-white);
}
.page { padding: 0; }

/* ── Faixa de marca ─────────────────────────────────────────────────────── */
.gh-band {
  background: var(--logo-purple);
  color: #fff;
  padding: 14mm 0 9mm;
  margin-bottom: 10mm;
}
.gh-logo { height: 34px; width: auto; display: block; margin-bottom: 9mm; }
.gh-kicker {
  font-size: 8pt; font-weight: 600; text-transform: uppercase;
  letter-spacing: 1.6px; color: rgba(255,255,255,0.82); margin: 0;
}

/* ── Tipografia ─────────────────────────────────────────────────────────── */
h1 {
  color: var(--primary-purple); font-size: 21pt; font-weight: 800;
  letter-spacing: -0.4px; line-height: 1.2; margin: 0 0 4mm;
}
h2 {
  color: var(--purple-700); font-size: 13.5pt; font-weight: 700;
  margin: 9mm 0 3mm; padding-bottom: 2mm;
  border-bottom: 2px solid var(--primary-purple);
}
h3 {
  color: var(--purple-600); font-size: 11.5pt; font-weight: 600;
  margin: 6mm 0 2mm;
}
h4 { color: var(--text-strong); font-size: 10.5pt; font-weight: 600; margin: 5mm 0 2mm; }
p { margin: 2.5mm 0; }
strong { color: var(--text-strong); font-weight: 600; }
a { color: var(--primary-purple); text-decoration: none; }

/* Identificadores no corpo do texto: sem caixa, sem moldura, sem cor de alerta.
   Num documento de acompanhamento a marcação vermelha lia como erro e picotava
   a leitura — o nome do arquivo é só um nome. Distinção fica no peso. */
code {
  font-family: inherit;
  font-weight: 600;
  color: var(--text-strong);
  background: none;
  border: none;
  padding: 0;
  font-size: inherit;
}
/* Dentro de título, o identificador acompanha a cor e o peso do título — senão
   sai um pedaço preto no meio de uma linha roxa. */
h1 code, h2 code, h3 code, h4 code { color: inherit; font-weight: inherit; }

pre {
  background: var(--bg-subtle); border: 1px solid var(--linha);
  border-left: 3px solid var(--primary-purple);
  border-radius: 6px; padding: 3mm 4mm;
}
pre code {
  font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', monospace;
  font-weight: 400; font-size: 9pt; color: var(--text-body);
}

blockquote {
  margin: 4mm 0; padding: 3mm 5mm; background: var(--bg-subtle);
  border-left: 3px solid var(--primary-purple); color: var(--text-muted);
  font-size: 9.5pt;
}
blockquote p { margin: 0; }

ul, ol { margin: 2.5mm 0; padding-left: 6mm; }
li { margin: 1.5mm 0; }
li::marker { color: var(--primary-purple); }

/* ── Tabelas ────────────────────────────────────────────────────────────── */
table { border-collapse: collapse; width: 100%; margin: 4mm 0; font-size: 9.5pt; }
th {
  background: var(--primary-purple); color: #fff; text-align: left;
  padding: 2.5mm 3mm; font-weight: 600;
}
td { border: 1px solid var(--linha); padding: 2.2mm 3mm; vertical-align: top; }
tbody tr:nth-child(even) { background: var(--bg-subtle); }
table code { color: var(--purple-700); }

hr { border: none; border-top: 1px solid var(--linha-forte); margin: 8mm 0 4mm; }
hr + p em, hr + p { color: var(--text-muted); font-size: 9pt; }

/* ── Impressão ──────────────────────────────────────────────────────────── */
@page {
  size: A4;
  margin: 16mm 16mm 16mm 16mm;
}
/* A faixa sangra até a borda física da página nos três lados, o texto respeita
   a margem. Sem o -16mm no topo sobra uma tira branca acima do roxo, que faz a
   faixa parecer desalinhada em vez de proposital. */
.gh-band {
  margin: -16mm -16mm 10mm;
  padding: 16mm 16mm 9mm;
}

h2, h3, h4 { page-break-after: avoid; break-after: avoid; }
tr, blockquote, pre { page-break-inside: avoid; break-inside: avoid; }
thead { display: table-header-group; }
li { break-inside: avoid; }
`;

const html = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>${esc(h1)}</title>
<style>${css}</style>
</head>
<body>
<div class="page">
<div class="gh-band">${logoTag}<p class="gh-kicker">${esc(kicker)}</p></div>
${body}
</div>
</body>
</html>`;

writeFileSync(outPath, html, 'utf8');
console.log('HTML gerado em ' + outPath);
