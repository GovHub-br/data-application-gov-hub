// Converte um relatório Markdown (GFM) em HTML autocontido, estilizado para
// impressão em A4. Uso:
//   node build_report.mjs <entrada.md> <saida.html> ["Texto da tag da capa"]
//
// Depende do pacote `marked` instalado no diretório deste script.
import { readFileSync, writeFileSync } from 'fs';
import { marked } from 'marked';

const [, , mdPath, outPath, tagArg] = process.argv;
if (!mdPath || !outPath) {
  console.error('Uso: node build_report.mjs <entrada.md> <saida.html> ["tag da capa"]');
  process.exit(1);
}

const md = readFileSync(mdPath, 'utf8');
const body = marked.parse(md, { gfm: true, breaks: false });

// Título da aba do navegador: primeira linha "# ..." do markdown, se houver.
const h1 = (md.match(/^#\s+(.+)$/m) || [])[1] || 'Relatório de Prestação de Contas';
const tag = tagArg || 'Prestação de Contas';

const css = `
:root {
  --azul: #0b3d6b;
  --azul-claro: #1b6ca8;
  --cinza: #444;
  --cinza-claro: #f4f6f9;
  --borda: #d7dde5;
}
* { box-sizing: border-box; }
body {
  font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
  color: #222; line-height: 1.55; font-size: 11pt; margin: 0;
}
.page { max-width: 820px; margin: 0 auto; padding: 32px 36px; }
.capa { border-bottom: 4px solid var(--azul); padding-bottom: 18px; margin-bottom: 8px; }
.capa .tag {
  display: inline-block; background: var(--azul); color: #fff; font-size: 9pt;
  letter-spacing: 1px; text-transform: uppercase; padding: 4px 12px;
  border-radius: 3px; margin-bottom: 14px;
}
h1 { color: var(--azul); font-size: 23pt; margin: 6px 0 4px; line-height: 1.2; }
h2 { color: var(--azul); font-size: 15pt; margin-top: 28px; padding-bottom: 6px; border-bottom: 2px solid var(--azul-claro); }
h3 { color: var(--azul-claro); font-size: 12.5pt; margin-top: 20px; }
h4 { color: var(--cinza); font-size: 11pt; margin-top: 16px; }
p { margin: 8px 0; }
strong { color: #111; }
a { color: var(--azul-claro); text-decoration: none; }
code {
  background: var(--cinza-claro); border: 1px solid var(--borda); border-radius: 3px;
  padding: 1px 5px; font-family: "Consolas", "Courier New", monospace; font-size: 9.5pt; color: #b03060;
}
blockquote {
  margin: 14px 0; padding: 10px 16px; background: var(--cinza-claro);
  border-left: 4px solid var(--azul-claro); color: var(--cinza); font-size: 10pt;
}
blockquote p { margin: 0; }
ul { margin: 8px 0; padding-left: 22px; }
li { margin: 3px 0; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 9.5pt; }
th { background: var(--azul); color: #fff; text-align: left; padding: 7px 10px; font-weight: 600; }
td { border: 1px solid var(--borda); padding: 6px 10px; vertical-align: top; }
tbody tr:nth-child(even) { background: var(--cinza-claro); }
table code { background: transparent; border: none; padding: 0; color: var(--azul); }
hr { border: none; border-top: 1px solid var(--borda); margin: 22px 0; }
@page { size: A4; margin: 16mm 14mm 18mm 14mm; }
@media print {
  .page { padding: 0; max-width: none; }
  h2, h3, h4 { page-break-after: avoid; }
  tr, blockquote { page-break-inside: avoid; }
  thead { display: table-header-group; }
}
`;

const html = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>${h1}</title>
<style>${css}</style>
</head>
<body>
<div class="page">
<div class="capa"><span class="tag">${tag}</span></div>
${body}
</div>
</body>
</html>`;

writeFileSync(outPath, html, 'utf8');
console.log('HTML gerado em ' + outPath);
