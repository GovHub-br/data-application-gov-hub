"""Descoberta oficial de agencia/conta dos entes via API do Transferegov.

Codigo de extracao fornecido pelo time (fonte de verdade para
`programa` -> `plano_acao` -> `plano_acao_dado_bancario`), integrado como
plugin flat do repositorio. A lista de codigos de programa a consultar vem
de fora (Variavel do Airflow, lida pela DAG) -- este modulo permanece Python
puro, sem depender de arquivo local nem do Airflow, para continuar facil de
testar fora do container. O restante da logica (paginacao, parsing de
Content-Range, selecao de conta ativa etc.) esta inalterado.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
from pathlib import Path

import pandas as pd
import requests

import config_bsc_pnab as settings

DEFAULT_LOG_DIR = settings.TRANSFEREGOV_LOG_DIR
LOGGER_NAME = "transferegov_api"
LOGGER = logging.getLogger(LOGGER_NAME)


def configure_logger(
    log_dir: str | Path | None = DEFAULT_LOG_DIR,
    log_to_file: bool = True,
) -> logging.Logger:
    """Configura logs no console e, opcionalmente, em arquivo."""

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if log_to_file:
        if log_dir is None:
            raise ValueError("log_dir deve ser informado quando log_to_file=True.")

        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / (
            f"transferegov_api__{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_to_file:
        logger.info("Logger inicializado em %s", log_path)
    else:
        logger.info("Logger inicializado no console")

    return logger


def _get_logger(logger: logging.Logger | None = None) -> logging.Logger:
    """Retorna um logger configurado para acompanhar a execução."""

    if logger is not None:
        return logger

    if LOGGER.handlers:
        return LOGGER

    return configure_logger(log_to_file=False)


def _parse_content_range(
    content_range: str | None,
) -> tuple[int | None, int | None, int | None]:
    """Extrai início, fim e total do header Content-Range."""

    if not content_range or "/" not in content_range:
        return None, None, None

    range_part, total_part = content_range.split("/", maxsplit=1)
    total = int(total_part) if total_part.isdigit() else None

    if "-" not in range_part:
        return None, None, total

    start_part, end_part = range_part.split("-", maxsplit=1)
    if not start_part.isdigit() or not end_part.isdigit():
        return None, None, total

    return int(start_part), int(end_part), total


def _get_transferegov_paginated(
    url: str,
    params: dict,
    page_size: int = 1000,
    logger: logging.Logger | None = None,
) -> list[dict]:
    """Consulta endpoints paginados do Transferegov usando limit/offset."""

    if page_size <= 0:
        raise ValueError("page_size deve ser maior que zero.")

    registros = []
    offset = 0
    page_number = 1

    while True:
        page_params = {
            **params,
            "limit": page_size,
            "offset": offset,
        }

        response = requests.get(
            url,
            params=page_params,
            headers={"Prefer": "count=exact"},
            timeout=30,
        )
        response.raise_for_status()

        content_range = response.headers.get("Content-Range")
        _, range_end, total_registros = _parse_content_range(content_range)
        pagina = response.json()
        if not pagina:
            break

        registros.extend(pagina)

        if logger is not None:
            logger.info(
                "Página da API carregada | endpoint=%s | pagina=%s | offset=%s | "
                "registros_pagina=%s | registros_acumulados=%s | content_range=%s",
                url.rsplit("/", maxsplit=1)[-1],
                page_number,
                offset,
                len(pagina),
                len(registros),
                content_range,
            )

        if total_registros is not None and len(registros) >= total_registros:
            break

        if range_end is not None:
            next_offset = range_end + 1
            if next_offset <= offset:
                break
            offset = next_offset
        elif len(pagina) < page_size:
            break
        else:
            offset += page_size

        page_number += 1

    return registros


def get_programa_transferegov(
    id_programa: str,
    page_size: int = 1000,
) -> list[dict]:
    """
    Consulta a API do Transferegov para um id de programa especifico.

    ``id_programa`` e o ID interno do Transferegov (mesmo valor usado pela
    Variable ``transferegov_programas_ids`` e por
    ``ClienteTransfereGov.get_planos_acao_by_programa`` no pipeline
    ``transferegov_fundo_a_fundo``) -- nao o ``codigo_programa`` (codigo de
    negocio, campo diferente na tabela ``programa``).

    Parâmetros
    ----------
    id_programa : str
        ID interno do programa a ser consultado.

    Retorna
    -------
    list[dict]
        Resposta da API em formato JSON.
    """

    url = "https://api.transferegov.gestao.gov.br/fundoafundo/programa"

    params = {"id_programa": f"eq.{id_programa}"}

    return _get_transferegov_paginated(url, params, page_size=page_size)


def get_id_programa(id_programa: str) -> dict:
    programas = get_programa_transferegov(id_programa)

    if not programas:
        raise ValueError(
            f"Nenhum programa encontrado na API para id_programa={id_programa}."
        )

    programa_dict = programas[0]

    id_programa_dict = {
        "id_programa": programa_dict["id_programa"],
        "codigo_programa": programa_dict["codigo_programa"],
        "nome_programa": programa_dict["nome_programa"],
    }

    return id_programa_dict


def get_plano_acao_transferegov(
    id_programa: int | str,
    page_size: int = 1000,
    logger: logging.Logger | None = None,
) -> list[dict]:
    """
    Consulta a API do Transferegov e retorna os planos de ação
    vinculados a um id_programa.

    Parâmetros
    ----------
    id_programa : int | str
        ID interno do programa no Transferegov.

    Retorna
    -------
    list[dict]
        Resposta da API em formato JSON.
    """

    url = "https://api.transferegov.gestao.gov.br/fundoafundo/plano_acao"

    params = {"id_programa": f"eq.{id_programa}"}

    return _get_transferegov_paginated(
        url,
        params,
        page_size=page_size,
        logger=logger,
    )


def get_ids_plano_acao(
    id_programa: int | str,
    page_size: int = 1000,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    planos_acao = get_plano_acao_transferegov(
        id_programa,
        page_size=page_size,
        logger=logger,
    )

    ids_plano_acao = []

    for plano in planos_acao:
        plano_acao_dict = {
            "id_plano_acao": plano["id_plano_acao"],
            "id_programa": plano["id_programa"],
            "codigo_plano_acao": plano["codigo_plano_acao"],
            "nome_ente_recebedor_plano_acao": plano["nome_ente_recebedor_plano_acao"],
            "uf_ente_recebedor_plano_acao": plano["uf_ente_recebedor_plano_acao"],
            "codigo_ibge_municipio_ente_recebedor_plano_acao": plano[
                "codigo_ibge_municipio_ente_recebedor_plano_acao"
            ],
        }

        ids_plano_acao.append(plano_acao_dict)

    return pd.DataFrame(ids_plano_acao)


def get_dado_bancario_plano_acao(
    id_plano_acao: int | str,
    page_size: int = 1000,
) -> list[dict]:
    """
    Consulta a API do Transferegov e retorna os dados bancários
    vinculados a um plano de ação.

    Parâmetros
    ----------
    id_plano_acao : int | str
        ID do plano de ação.

    Retorna
    -------
    list[dict]
        Resposta da API em formato JSON.
    """

    url = "https://api.transferegov.gestao.gov.br/fundoafundo/plano_acao_dado_bancario"

    params = {"id_plano_acao": f"eq.{id_plano_acao}"}

    return _get_transferegov_paginated(url, params, page_size=page_size)


def get_agencia_conta(id_plano_acao: int | str) -> dict | None:
    dados_bancarios = get_dado_bancario_plano_acao(id_plano_acao)

    if not dados_bancarios:
        return None

    if len(dados_bancarios) > 1:
        contas_ativas = [
            conta
            for conta in dados_bancarios
            if conta.get("situacao_conta_plano_acao_dado_bancario") == "Conta Ativa"
        ]

        if contas_ativas:
            conta_selecionada = contas_ativas[-1]
        else:
            conta_selecionada = dados_bancarios[-1]
    else:
        conta_selecionada = dados_bancarios[0]

    agencia_conta_dict = {
        "id_plano_acao": conta_selecionada.get("id_plano_acao"),
        "numero_agencia_plano_acao_dado_bancario": conta_selecionada.get(
            "numero_agencia_plano_acao_dado_bancario"
        ),
        "numero_conta_plano_acao_dado_bancario": conta_selecionada.get(
            "numero_conta_plano_acao_dado_bancario"
        ),
        "situacao_conta_plano_acao_dado_bancario": conta_selecionada.get(
            "situacao_conta_plano_acao_dado_bancario"
        ),
    }

    return agencia_conta_dict


def get_contas_agencias_programas(
    codigos_programas: list[str],
    page_size: int = 1000,
    logger: logging.Logger | None = None,
    max_workers: int = 20,
) -> list[dict]:
    """
    Retorna contas e agências dos planos de ação dos programas informados.

    ``codigos_programas`` recebe, apesar do nome, os ``id_programa`` (ID
    interno do Transferegov) -- o mesmo valor guardado na Variable
    ``transferegov_programas_ids`` e usado diretamente por
    ``ClienteTransfereGov.get_planos_acao_by_programa`` no pipeline
    ``transferegov_fundo_a_fundo``. Nao e o ``codigo_programa`` (codigo de
    negocio, outro campo da tabela ``programa``).

    O fluxo é, para cada id em ``codigos_programas``:
    1. Consulta o programa no Transferegov (``get_id_programa``);
    2. Consulta os planos de ação vinculados (``get_ids_plano_acao``);
    3. Consulta a conta/agência de cada plano de ação (``get_agencia_conta``),
       em paralelo (``max_workers`` threads) -- programas grandes (5-6 mil
       planos) rodavam essa etapa em serie, um HTTP GET por plano.
    """

    logger = _get_logger(logger)
    logger.info(
        "Iniciando extração de contas e agências | programas=%s | page_size=%s | "
        "max_workers=%s",
        len(codigos_programas),
        page_size,
        max_workers,
    )

    registros = []
    total_contas_encontradas = 0
    total_planos_sem_conta = 0

    for indice_programa, id_programa_alvo in enumerate(codigos_programas, start=1):
        logger.info(
            "Consultando programa %s/%s | id_programa=%s",
            indice_programa,
            len(codigos_programas),
            id_programa_alvo,
        )

        programa_api = get_id_programa(str(id_programa_alvo))
        logger.info(
            "Programa encontrado na API | id_programa=%s | codigo_programa=%s | nome=%s",
            programa_api["id_programa"],
            programa_api["codigo_programa"],
            programa_api["nome_programa"],
        )

        planos_acao = get_ids_plano_acao(
            programa_api["id_programa"],
            page_size=page_size,
            logger=logger,
        )
        total_planos = len(planos_acao)
        contas_encontradas_programa = 0
        planos_sem_conta_programa = 0

        logger.info(
            "Planos de ação encontrados | id_programa=%s | total_planos=%s",
            programa_api["id_programa"],
            total_planos,
        )

        planos_acao_records = planos_acao.to_dict("records")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_para_plano = {
                executor.submit(get_agencia_conta, plano_acao["id_plano_acao"]): plano_acao
                for plano_acao in planos_acao_records
            }

            for future in as_completed(future_para_plano):
                plano_acao = future_para_plano[future]
                agencia_conta = future.result() or {}

                if agencia_conta:
                    contas_encontradas_programa += 1
                    total_contas_encontradas += 1
                else:
                    planos_sem_conta_programa += 1
                    total_planos_sem_conta += 1

                registros.append(
                    {
                        "id_programa": programa_api["id_programa"],
                        "codigo_programa": programa_api["codigo_programa"],
                        "nome_programa": programa_api["nome_programa"],
                        "id_plano_acao": plano_acao["id_plano_acao"],
                        "agencia": agencia_conta.get(
                            "numero_agencia_plano_acao_dado_bancario"
                        ),
                        "conta": agencia_conta.get(
                            "numero_conta_plano_acao_dado_bancario"
                        ),
                        "situacao_conta": agencia_conta.get(
                            "situacao_conta_plano_acao_dado_bancario"
                        ),
                    }
                )

        logger.info(
            "Programa finalizado | id_programa=%s | planos=%s | contas=%s | sem_conta=%s",
            programa_api["id_programa"],
            total_planos,
            contas_encontradas_programa,
            planos_sem_conta_programa,
        )

    logger.info(
        "Extração finalizada | linhas=%s | contas=%s | sem_conta=%s",
        len(registros),
        total_contas_encontradas,
        total_planos_sem_conta,
    )

    return registros
