"""Estilo e helpers compartilhados pelos dois geradores HTML (dicionário
técnico e catálogo gestor) — identidade visual GovHub (gov-hub.io), tokens e
receitas de github.com/GovHub-br/GovHub-skills/tree/main/01-govhub/govhub-visual-identity.

Diferente do DOCX (aproximação via python-docx), aqui o CSS real da marca é
usado (gradientes, sombras, cards, tokens) — arquivo único autocontido
(logo embutida em base64, sem dependência externa além da fonte Inter via
Google Fonts, como na receita oficial).
"""

from __future__ import annotations

import base64
import html
from pathlib import Path
from typing import Any

ASSETS_DIR = Path(__file__).resolve().parents[1] / "assets"
LOGO_LIGHT = ASSETS_DIR / "logo_horizontal_light.png"
LOGO_PRIMARY = ASSETS_DIR / "logo_horizontal_primary.png"


def _logo_data_uri(path: Path) -> str:
    if not path.exists():
        return ""
    data = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{data}"


def esc(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


TOKENS_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

:root {
  --primary-purple:   #7A34F3;
  --secondary-purple: #8B5CF6;
  --accent-orange:    #F97316;
  --accent-orange-hover: #EA580C;
  --text-white:       #FFFFFF;
  --purple-400: #9249CA;
  --purple-600: #7C3AAD;
  --purple-700: #5B21B6;
  --text-strong: #202020;
  --text-body:   #2D3748;
  --text-muted:  #666666;
  --bg-white:  #FFFFFF;
  --bg-light:  #F7F7F7;
  --bg-subtle: #F8F9FA;
  --border-soft: #E3DAF3;
  --color-success:   #10B981;
  --color-highlight: #FFD700;
  --color-warm:      #F19F42;
  --font-family-base: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  --shadow-md: 0 2px 10px rgba(0,0,0,0.1);
  --shadow-lg: 0 4px 20px rgba(0,0,0,0.1);
  --shadow-xl: 0 8px 30px rgba(0,0,0,0.15);
  --transition-normal: all 0.2s ease;
  --radius-sm: 6px; --radius-md: 10px; --radius-lg: 16px;
}
"""

BASE_CSS = """
* { box-sizing: border-box; }
body {
  font-family: var(--font-family-base);
  color: var(--text-body);
  background: var(--bg-light);
  margin: 0;
}
h1, h2, h3, h4 { color: var(--primary-purple); font-weight: 700; }
h1 { font-size: 1.9rem; }
h2 { font-size: 1.4rem; margin-top: 2.5rem; border-bottom: 2px solid var(--border-soft); padding-bottom: 6px; }
h3 { font-size: 1.1rem; color: var(--purple-700); }
a { color: var(--purple-700); }
p { line-height: 1.55; }

.gh-report-cover {
  background: linear-gradient(135deg, var(--primary-purple) 0%, var(--purple-700) 100%);
  color: var(--text-white);
  padding: 64px 48px 48px;
  text-align: center;
}
.gh-report-cover img { height: 48px; margin-bottom: 24px; }
.gh-report-cover__kicker { text-transform: uppercase; letter-spacing: 2px; font-weight: 600; font-size: 0.8rem; opacity: 0.85; }
.gh-report-cover__title { color: var(--text-white); font-size: 2.4rem; font-weight: 800; margin: 10px 0; line-height: 1.15; }
.gh-report-cover__subtitle { font-size: 1.05rem; opacity: 0.95; max-width: 720px; margin: 0 auto; }
.gh-report-cover__bar { width: 64px; height: 4px; background: var(--accent-orange); margin: 24px auto 0; border-radius: 999px; }

.gh-layout { display: flex; align-items: flex-start; max-width: 1400px; margin: 0 auto; }
.gh-toc {
  position: sticky; top: 0; align-self: flex-start;
  width: 280px; min-width: 280px; height: 100vh; overflow-y: auto;
  background: var(--bg-white); border-right: 1px solid var(--border-soft);
  padding: 20px 16px;
}
.gh-toc input[type=search] {
  width: 100%; padding: 8px 12px; border: 1px solid var(--border-soft);
  border-radius: var(--radius-sm); font-family: var(--font-family-base);
  font-size: 0.85rem; margin-bottom: 14px;
}
.gh-toc__domain { font-weight: 700; color: var(--primary-purple); font-size: 0.8rem;
  text-transform: uppercase; letter-spacing: 0.5px; margin: 16px 0 6px; }
.gh-toc__link { display: block; font-size: 0.8rem; color: var(--text-body); text-decoration: none;
  padding: 3px 6px; border-radius: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.gh-toc__link:hover { background: var(--bg-subtle); color: var(--purple-700); }
.gh-toc__link.gh-flagged::after { content: " \\26A0"; color: var(--accent-orange); }

.gh-content { flex: 1; min-width: 0; padding: 36px 48px 80px; }

.gh-card {
  background: var(--bg-white);
  border-radius: var(--radius-md);
  padding: 22px 26px;
  box-shadow: var(--shadow-md);
  margin-bottom: 28px;
  scroll-margin-top: 16px;
}
.gh-card__title { color: var(--primary-purple); font-weight: 700; margin: 0 0 4px; }
.gh-card__subtitle { color: var(--text-muted); font-size: 0.85rem; font-style: italic; margin: 0 0 12px; }

.gh-badge {
  display: inline-block; background: var(--primary-purple); color: var(--text-white);
  font-size: 0.7rem; font-weight: 600; padding: 3px 10px; border-radius: 999px; margin: 0 6px 6px 0;
}
.gh-badge--accent  { background: var(--accent-orange); }
.gh-badge--muted   { background: var(--text-muted); }
.gh-badge--outline { background: transparent; color: var(--purple-700); border: 1px solid var(--purple-700); }

.gh-table { width: 100%; border-collapse: collapse; background: var(--bg-white);
  box-shadow: var(--shadow-md); border-radius: var(--radius-md); overflow: hidden; margin: 14px 0; }
.gh-table thead th { background: var(--primary-purple); color: var(--text-white);
  font-weight: 600; text-align: left; padding: 10px 12px; font-size: 0.8rem; }
.gh-table tbody td { padding: 9px 12px; color: var(--text-body); border-bottom: 1px solid #eee;
  font-size: 0.82rem; vertical-align: top; }
.gh-table tbody tr:nth-child(even) { background: var(--bg-subtle); }
.gh-table tbody tr:hover { background: rgba(122, 52, 243, 0.06); }
.gh-table--kv td:first-child { font-weight: 600; color: var(--purple-700); background: var(--bg-subtle); width: 260px; }

.gh-callout { border-left: 3px solid var(--accent-orange); background: #FFF7ED;
  padding: 10px 16px; border-radius: 0 var(--radius-sm) var(--radius-sm) 0; margin: 12px 0; font-size: 0.85rem; }
.gh-callout ul { margin: 4px 0 0; padding-left: 18px; }

.gh-domain-header { background: var(--bg-white); border-radius: var(--radius-md);
  padding: 20px 26px; box-shadow: var(--shadow-md); margin: 40px 0 20px; border-top: 4px solid var(--primary-purple); }

footer.gh-footer { text-align: center; color: var(--text-muted); font-size: 0.75rem; padding: 30px; }
@media print { .gh-toc { display: none; } .gh-layout { display: block; } }
"""

SEARCH_JS = """
document.addEventListener('DOMContentLoaded', function () {
  var input = document.getElementById('gh-search');
  if (!input) return;
  input.addEventListener('input', function () {
    var q = input.value.toLowerCase();
    document.querySelectorAll('.gh-toc__link').forEach(function (a) {
      var text = a.dataset.search || a.textContent;
      a.style.display = text.toLowerCase().indexOf(q) === -1 ? 'none' : 'block';
    });
    document.querySelectorAll('.gh-toc__domain').forEach(function (d) {
      var next = d.nextElementSibling, any = false;
      while (next && !next.classList.contains('gh-toc__domain')) {
        if (next.style.display !== 'none') any = true;
        next = next.nextElementSibling;
      }
      d.style.display = any ? 'block' : 'none';
    });
  });
});
"""


def page_shell(*, title: str, body: str, extra_css: str = "") -> str:
    return f"""<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>{TOKENS_CSS}{BASE_CSS}{extra_css}</style>
</head>
<body>
{body}
<script>{SEARCH_JS}</script>
</body>
</html>"""


def render_cover(title: str, subtitle: str, kicker: str = "GovHub · SALIC") -> str:
    logo_uri = _logo_data_uri(LOGO_LIGHT)
    logo_html = f'<img src="{logo_uri}" alt="GovHub">' if logo_uri else ""
    return f"""
<header class="gh-report-cover">
  {logo_html}
  <div class="gh-report-cover__kicker">{esc(kicker)}</div>
  <h1 class="gh-report-cover__title">{esc(title)}</h1>
  <p class="gh-report-cover__subtitle">{esc(subtitle)}</p>
  <div class="gh-report-cover__bar"></div>
</header>
"""


def render_kv_table(rows: list[tuple[str, str]]) -> str:
    trs = "".join(
        f"<tr><td>{esc(label)}</td><td>{value if value else '&mdash;'}</td></tr>"
        for label, value in rows
    )
    return f'<table class="gh-table gh-table--kv"><tbody>{trs}</tbody></table>'


def render_badge(text: str, variant: str = "") -> str:
    cls = f"gh-badge gh-badge--{variant}" if variant else "gh-badge"
    return f'<span class="{cls}">{esc(text)}</span>'


def render_bullet_list(items: list[str]) -> str:
    lis = "".join(f"<li>{esc(item)}</li>" for item in items)
    return f"<ul>{lis}</ul>"
