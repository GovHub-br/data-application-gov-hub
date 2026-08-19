"""Orquestra o pipeline completo de documentação do SALIC, do parse do
dicionário original até os artefatos finais: YAML semântico, DOCX + HTML
(técnico e gestor), e as declarações dbt `sources:` (schema.yml + meta
OpenMetadata) em dbt/minc/models/salic_bronze/.

Cada etapa é um script independente (podem ser rodados avulsos para
depuração); este script só os executa em sequência, na ordem certa, parando
no primeiro erro. O perfilamento (etapa 2) é o passo caro e é resumível por
natureza — reexecutar o pipeline pula tabelas já perfiladas, a menos que
--force-profile seja passado.

Uso:
    poetry run python scripts/salic_docs/pipeline.py
    poetry run python scripts/salic_docs/pipeline.py --tables sac__abrangencia,agentes__agentes
    poetry run python scripts/salic_docs/pipeline.py --skip-dict --force-profile
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent


def _run(step: str, args: list[str]) -> None:
    print(f"\n{'=' * 70}\n[pipeline] {step}\n{'=' * 70}", flush=True)
    t0 = time.time()
    result = subprocess.run([sys.executable, *args], cwd=SCRIPTS_DIR.parents[1])
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"[pipeline] FALHOU em '{step}' (código {result.returncode}, {elapsed:.1f}s)", flush=True)
        raise SystemExit(result.returncode)
    print(f"[pipeline] OK: {step} ({elapsed:.1f}s)", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dict-path", type=str, default=None, help="caminho do dicionário SchemaSpy (padrão definido em 01_parse_dictionary.py)")
    parser.add_argument("--tables", type=str, default=None, help="lista separada por vírgula; roda o pipeline só para essas tabelas (smoke test)")
    parser.add_argument("--skip-dict", action="store_true", help="pula o parse do dicionário original (reusa output/dictionary.json existente)")
    parser.add_argument("--skip-profile", action="store_true", help="pula o perfilamento do bronze (reusa output/profile/*.json existente)")
    parser.add_argument("--force-profile", action="store_true", help="reperfila mesmo as tabelas que já têm checkpoint salvo")
    parser.add_argument("--dicionario-docx-out", type=str, default=None, help="caminho alternativo para o .docx do dicionário técnico (ex.: smoke test)")
    parser.add_argument("--catalogo-docx-out", type=str, default=None, help="caminho alternativo para o .docx do catálogo gestor (ex.: smoke test)")
    parser.add_argument("--dicionario-html-out", type=str, default=None, help="caminho alternativo para o .html do dicionário técnico (ex.: smoke test)")
    parser.add_argument("--catalogo-html-out", type=str, default=None, help="caminho alternativo para o .html do catálogo gestor (ex.: smoke test)")
    args = parser.parse_args()

    t_start = time.time()

    if not args.skip_dict:
        dict_args = [str(SCRIPTS_DIR / "01_parse_dictionary.py")]
        if args.dict_path:
            dict_args.append(args.dict_path)
        _run("1/9 — parse do dicionário de dados original (SchemaSpy)", dict_args)
    else:
        print("[pipeline] pulando etapa 1 (--skip-dict)")

    if not args.skip_profile:
        profile_args = [str(SCRIPTS_DIR / "02_profile_bronze.py")]
        if args.tables:
            profile_args += ["--tables", args.tables]
        if args.force_profile:
            profile_args.append("--force")
        _run("2/9 — perfilamento estatístico do schema bronze", profile_args)
    else:
        print("[pipeline] pulando etapa 2 (--skip-profile)")

    _run("3/9 — junção dicionário + perfil, sinalização de tabelas suspeitas", [str(SCRIPTS_DIR / "03_flag_and_merge.py")])
    _run("4/9 — geração do YAML semântico (por schema)", [str(SCRIPTS_DIR / "04_build_yaml.py")])

    dic_docx_args = [str(SCRIPTS_DIR / "05_generate_dicionario_dados.py")]
    if args.tables:
        dic_docx_args += ["--tables", args.tables]
    if args.dicionario_docx_out:
        dic_docx_args += ["--out", args.dicionario_docx_out]
    _run("5/9 — geração do Dicionário de Dados (DOCX técnico)", dic_docx_args)

    cat_docx_args = [str(SCRIPTS_DIR / "06_generate_catalogo_dados.py")]
    if args.tables:
        cat_docx_args += ["--tables", args.tables]
    if args.catalogo_docx_out:
        cat_docx_args += ["--out", args.catalogo_docx_out]
    _run("6/9 — geração do Catálogo de Dados (DOCX gestor)", cat_docx_args)

    dic_html_args = [str(SCRIPTS_DIR / "07_generate_html_dicionario.py")]
    if args.tables:
        dic_html_args += ["--tables", args.tables]
    if args.dicionario_html_out:
        dic_html_args += ["--out", args.dicionario_html_out]
    _run("7/9 — geração do Dicionário de Dados (HTML técnico)", dic_html_args)

    cat_html_args = [str(SCRIPTS_DIR / "08_generate_html_catalogo.py")]
    if args.tables:
        cat_html_args += ["--tables", args.tables]
    if args.catalogo_html_out:
        cat_html_args += ["--out", args.catalogo_html_out]
    _run("8/9 — geração do Catálogo de Dados (HTML gestor)", cat_html_args)

    _run(
        "9/9 — geração dos sources dbt (schema.yml + meta OpenMetadata)",
        [str(SCRIPTS_DIR / "09_generate_dbt_sources.py")],
    )

    total = time.time() - t_start
    print(f"\n[pipeline] concluído em {total / 60:.1f} min. Artefatos finais em dbt/minc/docs/salic/", flush=True)


if __name__ == "__main__":
    main()
