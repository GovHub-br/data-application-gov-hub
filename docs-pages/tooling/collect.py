"""
collect.py: orquestra os coletores e grava o acervo em src/_data/.

Separado do build de propósito. A coleta usa rede (git, gh); o build não usa
nada. Assim o CI monta o site sem depender de rede, de VPN nem de banco — e o
diff de uma coleta mostra exatamente o que mudou no repositório no período.

Um coletor que falha não derruba os outros: o acervo anterior continua valendo,
e o build segue com ele.

Uso:  python -m tooling.collect
"""

from __future__ import annotations

import sys

from tooling.collectors import airflow_dags, dbt_models, entregas
from tooling.common import log

COLETORES = (
    ("dbt", dbt_models.coletar),
    ("airflow", airflow_dags.coletar),
    ("entregas", entregas.coletar),
)


def main() -> int:
    falhas = []
    for nome, funcao in COLETORES:
        try:
            funcao()
        except Exception as erro:  # noqa: BLE001 — falha de um não derruba os outros
            log.error("coletor %s falhou: %s", nome, erro)
            falhas.append(nome)

    if falhas:
        log.warning(
            "coleta terminou com %d falha(s): %s — o acervo anterior desses "
            "continua valendo",
            len(falhas),
            ", ".join(falhas),
        )
    else:
        log.info("coleta completa")
    return 0


if __name__ == "__main__":
    sys.exit(main())
