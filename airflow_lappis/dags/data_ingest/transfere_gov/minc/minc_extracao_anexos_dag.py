import gc
import inspect
import io
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from cliente_postgres import ClientPostgresDB
from extracao_planilhas import extrair_tabela_raw, extrair_pnab, extrair_lpg, TimeoutLeituraError
from postgres_helpers import get_postgres_conn

_PROGRAMAS = [
    {
        "nome_programa": "LPG",
        "id_programa": [46, 47],
        "regex_header": r"edita(?:is|l)",
        "regex_flags": "IGNORECASE",
        "col_header_idx": 1,
        "col_categoria_idx": 0,
        "min_len_categoria": 6,
        "bucket": "anexos-lpg",
        "prefix": "",
        "schema": "transferegov_fundo_a_fundo",
        "table": None,  # LPG usa roteamento por template — tabela definida dinamicamente
    },
    {
        "nome_programa": "PNAB",
        "id_programa": [60, 61, 62],
        "regex_header": r"contemplad|contratad|acompanhament|plan|sheet|dados|tabela|resumo|relat[oó]rio",
        "regex_flags": "IGNORECASE",
        "col_header_idx": 1,
        "col_categoria_idx": 0,
        "min_len_categoria": 6,
        "bucket": "anexos-pnab",
        "prefix": "",
        "schema": "transferegov_fundo_a_fundo",
        "table": None,  # PNAB usa roteamento por aba — tabela definida dinamicamente
    },
]

_S3_CONN_ID = "minio_default"

default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

_CHUNK_SIZE = 50
_MAX_FILE_BYTES = 10 * 1024 * 1024  # 10 MB — limite para evitar OOM no Worker


@dag(
    dag_id="minc_extracao_anexos_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transfere_gov", "anexos", "planilhas", "raw"],
)
def minc_extracao_anexos_dag() -> None:
    """DAG de extração de tabelas de anexos (XLSX/XLS/XLSM) para os
    programas LPG e PNAB, com Dynamic Task Mapping por **lote** de arquivos.

    Fluxo:

    1. ``listar_arquivos_s3`` — descobre todos os arquivos de planilha nos
       buckets do MinIO, enriquece cada entrada com os parâmetros do
       programa (regex, bucket, tabela destino) e fatia a lista em blocos
       (chunks) de 50 arquivos. Retorna ``list[list[dict]]``.
       NOTA: .ods é intencionalmente excluído — a engine odf causa OOM.
    2. ``baixar_e_extrair`` — para CADA lote (via ``.expand()``), itera
       sobre os arquivos do lote. Para cada arquivo, baixa do MinIO para
       memória, extrai as subtabelas e insere no PostgreSQL. Um
       ``try-except`` por arquivo E por subtabela garante resiliência:
       se uma subtabela falhar no INSERT, as demais continuam. Conexões
       S3 e Postgres são criadas uma vez por lote e reutilizadas.
    3. ``fechar_pipeline`` — task de fechamento que consolida os
       resumos dos lotes e encerra a DAG. Usa trigger_rule=ALL_DONE
       para executar mesmo que alguns lotes falhem.
    """

    @task
    def listar_arquivos_s3() -> list[list[dict[str, Any]]]:
        """Lista arquivos de planilha em cada bucket do MinIO, enriquece
        cada entrada com os parâmetros de extração do programa e fatia
        a lista em blocos (chunks) de 50 arquivos.

        Retorna ``list[list[dict]]`` para que o Dynamic Task Mapping crie
        uma task por *lote* ao invés de uma task por *arquivo*, reduzindo
        ~10.000 tasks para ~200 e evitando OOM/timeout no Worker.

        NOTA: extensão .ods é excluída propositalmente — a engine odf
        carrega o DOM XML inteiro em memória e causa OOM Kills.
        """
        ids_validos = set(
            Variable.get(
                "transferegov_programas_ids",
                default_var=[46, 47, 60, 61, 62],
                deserialize_json=True,
            )
        )

        hook = S3Hook(aws_conn_id=_S3_CONN_ID)
        arquivos_meta: list[dict[str, Any]] = []

        for prog in _PROGRAMAS:
            ids_prog = prog.get("id_programa", [])
            if isinstance(ids_prog, int):
                ids_prog = [ids_prog]
            if ids_prog and not any(i in ids_validos for i in ids_prog):
                logging.info(
                    "[minc_extracao_anexos_dag.py] Programa %s (IDs %s) "
                    "nao esta em transferegov_programas_ids — pulando",
                    prog["nome_programa"],
                    ids_prog,
                )
                continue

            nome = prog["nome_programa"]
            bucket = prog["bucket"]
            prefix = prog.get("prefix", "")

            logging.info(
                "[minc_extracao_anexos_dag.py] Listando arquivos para programa %s "
                "no bucket s3://%s/%s",
                nome,
                bucket,
                prefix,
            )

            try:
                keys = hook.list_keys(
                    bucket_name=bucket,
                    prefix=prefix,
                )
            except Exception as exc:
                logging.warning(
                    "[minc_extracao_anexos_dag.py] Erro ao listar bucket %s: %s — pulando",
                    bucket,
                    exc,
                )
                continue

            if not keys:
                logging.warning(
                    "[minc_extracao_anexos_dag.py] Nenhum arquivo encontrado no "
                    "bucket %s para o programa %s",
                    bucket,
                    nome,
                )
                continue

            # .ods removido intencionalmente — engine odf causa OOM
            extensoes_validas = {".xlsx", ".xls", ".xlsm", ".xlsb"}

            for key in keys:
                ext = Path(key).suffix.lower()
                if ext not in extensoes_validas:
                    if ext == ".ods":
                        logging.info(
                            "[minc_extracao_anexos_dag.py] Arquivo .ods "
                            "ignorado (OOM risk): s3://%s/%s",
                            bucket,
                            key,
                        )
                    continue
                if Path(key).name.startswith("~$"):
                    continue

                arquivos_meta.append({
                    "key": key,
                    "bucket": bucket,
                    "nome_programa": nome,
                    "regex_header": prog["regex_header"],
                    "regex_flags": prog.get("regex_flags", "IGNORECASE"),
                    "col_header_idx": prog.get("col_header_idx", 1),
                    "col_categoria_idx": prog.get("col_categoria_idx", 0),
                    "min_len_categoria": prog.get("min_len_categoria", 6),
                    "schema": prog["schema"],
                    "table": prog["table"],
                })

            logging.info(
                "[minc_extracao_anexos_dag.py] Programa %s: %d arquivos encontrados",
                nome,
                sum(
                    1
                    for a in arquivos_meta
                    if a["nome_programa"] == nome
                ),
            )

        if not arquivos_meta:
            logging.warning(
                "[minc_extracao_anexos_dag.py] Nenhum arquivo encontrado em nenhum programa"
            )
            return []

        # ── Chunking: fatia a lista em blocos de _CHUNK_SIZE ──
        chunks: list[list[dict[str, Any]]] = [
            arquivos_meta[i : i + _CHUNK_SIZE]
            for i in range(0, len(arquivos_meta), _CHUNK_SIZE)
        ]

        logging.info(
            "[minc_extracao_anexos_dag.py] %d arquivos → %d lotes de até %d arquivos",
            len(arquivos_meta),
            len(chunks),
            _CHUNK_SIZE,
        )

        return chunks

    def _processar_arquivo(
        file_meta: dict[str, Any],
        s3_hook: S3Hook,
        db: ClientPostgresDB,
    ) -> dict[str, Any]:
        """Processa um único arquivo: download S3 → extração → INSERT.

        O roteamento é explícito por programa:
        - PNAB → extrair_pnab (roteamento por aba)
        - LPG → extrair_lpg (roteamento por template)
        - else → extrair_tabela_raw (fallback genérico via regex)

        Retorna metadados leves (sem dados_json) para o resumo do lote.
        Nunca levanta exceção — erros são capturados e devolvidos no dict.
        """
        nome_programa = file_meta["nome_programa"]
        bucket = file_meta["bucket"]
        key = file_meta["key"]
        schema = file_meta["schema"]
        table = file_meta["table"]
        file_name = Path(key).name

        # ── Checagem de tamanho + download em chamada S3 única ──
        try:
            obj = s3_hook.get_key(key=key, bucket_name=bucket)
            tamanho_bytes = obj.content_length
        except Exception as exc:
            logging.warning(
                "[minc_extracao_anexos_dag.py] Não foi possível obter "
                "metadados de s3://%s/%s: %s — pulando arquivo",
                bucket,
                key,
                exc,
            )
            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": 0,
                "n_linhas_inseridas": 0,
                "status": "erro_download",
                "erro": f"Falha ao obter metadados S3: {exc!r}",
            }

        tamanho_mb = tamanho_bytes / (1024 * 1024)

        if tamanho_bytes > _MAX_FILE_BYTES:
            logging.warning(
                "[minc_extracao_anexos_dag.py] Arquivo '%s' ignorado — "
                "%.2f MB excede o limite de %d MB (OOM protection)",
                file_name,
                tamanho_mb,
                _MAX_FILE_BYTES // (1024 * 1024),
            )
            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": 0,
                "n_linhas_inseridas": 0,
                "status": "erro_tamanho",
                "erro": (
                    f"Arquivo muito grande ({tamanho_mb:.2f}MB, "
                    f"limite {_MAX_FILE_BYTES // (1024 * 1024)}MB)"
                ),
            }

        logging.info(
            "[minc_extracao_anexos_dag.py] Baixando s3://%s/%s (%.2f MB) para memória",
            bucket,
            key,
            tamanho_mb,
        )

        # ── Download — reutiliza o obj já obtido (sem 2a chamada get_key) ──
        try:
            file_content = obj.get()["Body"].read()
        except Exception as exc:
            logging.error(
                "[minc_extracao_anexos_dag.py] Erro ao baixar s3://%s/%s: %s",
                bucket,
                key,
                exc,
            )
            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": 0,
                "n_linhas_inseridas": 0,
                "status": "erro_download",
                "erro": repr(exc),
            }

        buffer = io.BytesIO(file_content)
        # Libera referência ao conteúdo bruto logo após criar o buffer
        del file_content

        try:
            id_anexo = Path(key).stem  # nome do arquivo sem extensão

            # ── Roteamento explícito por programa ──
            if nome_programa == "PNAB":
                # ── Fluxo PNAB: roteamento por aba ──
                logging.info(
                    "[minc_extracao_anexos_dag.py] Extraindo PNAB '%s' "
                    "com roteamento por aba",
                    file_name,
                )
                resultados = extrair_pnab(
                    file_buffer=buffer,
                    file_name=file_name,
                    id_anexo=id_anexo,
                )

            elif nome_programa == "LPG":
                # ── Fluxo LPG: roteamento por template ──
                logging.info(
                    "[minc_extracao_anexos_dag.py] Extraindo LPG '%s' "
                    "com roteamento por template",
                    file_name,
                )
                resultados = extrair_lpg(
                    file_buffer=buffer,
                    file_name=file_name,
                    id_anexo=id_anexo,
                )

            else:
                # ── Fallback genérico: regex âncora + tabela única ──
                flags = 0
                for flag_name in file_meta.get("regex_flags", "IGNORECASE").split("|"):
                    flag_val = getattr(re, flag_name.strip(), None)
                    if flag_val is not None:
                        flags |= flag_val

                regex_header = re.compile(file_meta["regex_header"], flags)

                logging.info(
                    "[minc_extracao_anexos_dag.py] Extraindo '%s' "
                    "(fallback regex=%s, tabela=%s)",
                    file_name,
                    file_meta["regex_header"],
                    table,
                )

                _fn_params = inspect.signature(extrair_tabela_raw).parameters
                _extra_kwargs = (
                    {"file_name": file_name} if "file_name" in _fn_params else {}
                )
                resultados_fallback = extrair_tabela_raw(
                    file_path=buffer,
                    regex_header=regex_header,
                    col_header_idx=file_meta.get("col_header_idx", 1),
                    col_categoria_idx=file_meta.get("col_categoria_idx", 0),
                    min_len_categoria=file_meta.get("min_len_categoria", 6),
                    **_extra_kwargs,
                )

                # Converte formato do fallback para o formato padrao
                resultados = []
                for res_fb in resultados_fallback:
                    df_fb = res_fb["dados"]
                    df_fb = df_fb.loc[:, ~df_fb.columns.duplicated()]

                    # --- Data Cleaning ---
                    colunas_antes = len(df_fb.columns)
                    linhas_antes = len(df_fb)

                    df_fb = df_fb.dropna(axis=1, how="all")
                    df_fb = df_fb.dropna(how="all")

                    _col_meta_fb = {"tipo_edital"}
                    col_dados_fb = [c for c in df_fb.columns if c not in _col_meta_fb]
                    if col_dados_fb:
                        thresh_fb = max(1, int(len(col_dados_fb) * 0.3))
                        df_fb = df_fb.dropna(subset=col_dados_fb, thresh=thresh_fb)

                    df_fb = df_fb.reset_index(drop=True)

                    linhas_removidas = linhas_antes - len(df_fb)
                    colunas_removidas = colunas_antes - len(df_fb.columns)
                    if linhas_removidas or colunas_removidas:
                        logging.info(
                            "[minc_extracao_anexos_dag.py] Limpeza '%s' aba '%s': "
                            "removidas %d/%d linhas, %d/%d colunas",
                            file_name,
                            res_fb["aba"],
                            linhas_removidas,
                            linhas_antes,
                            colunas_removidas,
                            colunas_antes,
                        )

                    if df_fb.empty:
                        logging.warning(
                            "Aba %s descartada por estar vazia após limpeza.",
                            res_fb["aba"],
                        )
                        continue

                    df_fb["nome_arquivo"] = res_fb["nome_arquivo"]
                    df_fb["aba"] = res_fb["aba"]
                    df_fb["tipo_edital"] = res_fb.get("tipo_edital")
                    df_fb["nome_programa"] = nome_programa
                    df_fb["dt_ingest"] = datetime.now().isoformat()

                    resultados.append({
                        "nome_tabela_destino": table,
                        "dataframe": df_fb,
                    })

            # ── Inserção comum para PNAB, LPG e fallback ──
            n_subtabelas = len(resultados)
            total_linhas = 0

            for res in resultados:
                # ── try-except por subtabela: INSERT falho não
                # quebra as demais subtabelas do mesmo arquivo ──
                try:
                    df = res["dataframe"]
                    tabela_destino = res["nome_tabela_destino"]
                    df = df.loc[:, ~df.columns.duplicated()]

                    # --- Data Cleaning ---
                    colunas_antes = len(df.columns)
                    linhas_antes = len(df)

                    df = df.dropna(axis=1, how="all")
                    df = df.dropna(how="all")

                    _col_meta = {"id_anexo", "tipo_edital", "categoria_edital", "categoria_contemplado"}
                    col_dados = [c for c in df.columns if c not in _col_meta]
                    if col_dados:
                        thresh_dados = max(1, int(len(col_dados) * 0.3))
                        df = df.dropna(subset=col_dados, thresh=thresh_dados)

                    df = df.reset_index(drop=True)

                    linhas_removidas = linhas_antes - len(df)
                    colunas_removidas = colunas_antes - len(df.columns)
                    if linhas_removidas or colunas_removidas:
                        logging.info(
                            "[minc_extracao_anexos_dag.py] Limpeza %s '%s' → %s: "
                            "removidas %d/%d linhas, %d/%d colunas",
                            nome_programa,
                            file_name,
                            tabela_destino,
                            linhas_removidas,
                            linhas_antes,
                            colunas_removidas,
                            colunas_antes,
                        )

                    if df.empty:
                        logging.warning(
                            "[minc_extracao_anexos_dag.py] %s '%s' → %s: "
                            "descartada por estar vazia após limpeza",
                            nome_programa,
                            file_name,
                            tabela_destino,
                        )
                        continue

                    df["nome_arquivo"] = file_name
                    df["nome_programa"] = nome_programa
                    df["dt_ingest"] = datetime.now().isoformat()

                    linhas = df.to_dict(orient="records")

                    logging.info(
                        "[minc_extracao_anexos_dag.py] %s '%s' → %s: "
                        "inserindo %d registros em %s.%s",
                        nome_programa,
                        file_name,
                        tabela_destino,
                        len(df),
                        schema,
                        tabela_destino,
                    )
                    db.insert_data_por_tabela(
                        linhas,
                        table_name=tabela_destino,
                        schema=schema,
                    )
                    total_linhas += len(linhas)
                except Exception as exc_sub:
                    logging.warning(
                        "[minc_extracao_anexos_dag.py] %s '%s' → subtabela "
                        "'%s' falhou no INSERT: %s — continuando",
                        nome_programa,
                        file_name,
                        res.get("nome_tabela_destino", "?"),
                        exc_sub,
                    )
                    continue

            logging.info(
                "[minc_extracao_anexos_dag.py] %s '%s': %d subtabelas, "
                "%d linhas inseridas",
                nome_programa,
                file_name,
                n_subtabelas,
                total_linhas,
            )

            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": n_subtabelas,
                "n_linhas_inseridas": total_linhas,
                "status": "sucesso",
                "erro": None,
            }

        except TimeoutLeituraError as exc:
            logging.warning(
                "[minc_extracao_anexos_dag.py] Arquivo '%s' ignorado por "
                "lentidão (timeout %ds): %s",
                file_name,
                120,
                exc,
            )
            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": 0,
                "n_linhas_inseridas": 0,
                "status": "erro_timeout",
                "erro": repr(exc),
            }

        except Exception as exc:
            logging.warning(
                "[minc_extracao_anexos_dag.py] Arquivo ignorado por corrupção "
                "ou erro de leitura: %s | Erro: %s",
                file_name,
                exc,
            )
            return {
                "nome_programa": nome_programa,
                "nome_arquivo": file_name,
                "n_subtabelas": 0,
                "n_linhas_inseridas": 0,
                "status": "erro_extracao",
                "erro": repr(exc),
            }

        finally:
            # ── Limpeza de memória após cada arquivo ──
            # Libera buffer e força coleta de ciclos do Pandas
            buffer.close()
            del buffer
            gc.collect()

    @task
    def baixar_e_extrair(lote_de_arquivos: list[dict[str, Any]]) -> dict[str, Any]:
        """Processa um lote de arquivos do MinIO: download → extração → INSERT.

        Cada lote contém até 50 arquivos (definido por ``_CHUNK_SIZE``).
        A task itera sobre os arquivos com ``try-except`` individual — se
        um arquivo falhar (corrupção, erro de leitura, etc.), o erro é
        logado e o loop continua para o próximo arquivo do lote. O Worker
        não morre por causa de um único arquivo corrompido.

        Conexões S3 e Postgres são criadas uma vez por lote e reutilizadas
        em todos os arquivos, reduzindo o overhead de conexões.

        Returns
        -------
        dict[str, Any]
            Resumo consolidado do lote processado contendo contagens por
            status, total de linhas inseridas e lista de erros.
        """
        s3_hook = S3Hook(aws_conn_id=_S3_CONN_ID)
        db = ClientPostgresDB(get_postgres_conn())

        resumo: dict[str, Any] = {
            "n_arquivos_no_lote": len(lote_de_arquivos),
            "n_sucesso": 0,
            "n_erro_download": 0,
            "n_erro_extracao": 0,
            "n_erro_tamanho": 0,
            "n_erro_critico": 0,
            "n_erro_timeout": 0,
            "total_linhas_inseridas": 0,
            "erros": [],
        }

        for file_meta in lote_de_arquivos:
            try:
                resultado = _processar_arquivo(file_meta, s3_hook, db)
            except Exception as exc:
                # Erro inesperado que escapou do _processar_arquivo
                # (segurança extra — o Worker não pode morrer)
                file_name = Path(file_meta.get("key", "unknown")).name
                logging.error(
                    "[minc_extracao_anexos_dag.py] Erro crítico no arquivo '%s': %s",
                    file_name,
                    exc,
                )
                resumo["n_erro_critico"] += 1
                resumo["erros"].append({
                    "arquivo": file_name,
                    "status": "erro_critico",
                    "erro": repr(exc),
                })
                continue

            status = resultado.get("status", "desconhecido")
            if status == "sucesso":
                resumo["n_sucesso"] += 1
            elif status == "erro_download":
                resumo["n_erro_download"] += 1
            elif status == "erro_extracao":
                resumo["n_erro_extracao"] += 1
            elif status == "erro_tamanho":
                resumo["n_erro_tamanho"] += 1
            elif status == "erro_timeout":
                resumo["n_erro_timeout"] += 1
            else:
                resumo["n_erro_critico"] += 1

            resumo["total_linhas_inseridas"] += resultado.get("n_linhas_inseridas", 0)

            if resultado.get("erro"):
                resumo["erros"].append({
                    "arquivo": resultado.get("nome_arquivo", "unknown"),
                    "status": status,
                    "erro": resultado["erro"],
                })

        logging.info(
            "[minc_extracao_anexos_dag.py] Lote finalizado: %d/%d OK, "
            "%d erros download, %d erros extração, %d erros tamanho, "
            "%d erros críticos, %d linhas inseridas",
            resumo["n_sucesso"],
            resumo["n_arquivos_no_lote"],
            resumo["n_erro_download"],
            resumo["n_erro_extracao"],
            resumo["n_erro_tamanho"],
            resumo["n_erro_timeout"],
            resumo["n_erro_critico"],
            resumo["total_linhas_inseridas"],
        )

        return resumo

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def fechar_pipeline(resumos: list[dict[str, Any]]) -> dict[str, int]:
        """Task de fechamento que consolida os resumos dos lotes e encerra
        a DAG. Não faz INSERT — os dados já foram persistidos diretamente
        por cada ``baixar_e_extrair``.

        Usa trigger_rule=ALL_DONE para executar mesmo que alguns lotes
        falhem, garantindo que a DAG sempre gere o resumo consolidado.

        Returns
        -------
        dict[str, int]
            Contagem de arquivos por status e total de linhas inseridas.
        """
        contagem: dict[str, int] = {
            "n_sucesso": 0,
            "n_erro_download": 0,
            "n_erro_extracao": 0,
            "n_erro_tamanho": 0,
            "n_erro_critico": 0,
            "n_erro_timeout": 0,
            "total_linhas_inseridas": 0,
            "n_lotes": 0,
            "n_arquivos_total": 0,
        }

        for resumo in resumos:
            if resumo is None:
                continue
            contagem["n_lotes"] += 1
            contagem["n_sucesso"] += resumo.get("n_sucesso", 0)
            contagem["n_erro_download"] += resumo.get("n_erro_download", 0)
            contagem["n_erro_extracao"] += resumo.get("n_erro_extracao", 0)
            contagem["n_erro_tamanho"] += resumo.get("n_erro_tamanho", 0)
            contagem["n_erro_critico"] += resumo.get("n_erro_critico", 0)
            contagem["n_erro_timeout"] += resumo.get("n_erro_timeout", 0)
            contagem["total_linhas_inseridas"] += resumo.get("total_linhas_inseridas", 0)
            contagem["n_arquivos_total"] += resumo.get("n_arquivos_no_lote", 0)

        total_erros = (
            contagem["n_erro_download"]
            + contagem["n_erro_extracao"]
            + contagem["n_erro_tamanho"]
            + contagem["n_erro_timeout"]
            + contagem["n_erro_critico"]
        )

        if total_erros:
            logging.warning(
                "[minc_extracao_anexos_dag.py] %d arquivos com erro de download, "
                "%d com erro de extração, %d com erro de tamanho, "
                "%d com erro crítico",
                contagem["n_erro_download"],
                contagem["n_erro_extracao"],
                contagem["n_erro_tamanho"],
                contagem["n_erro_critico"],
            )

        logging.info(
            "[minc_extracao_anexos_dag.py] Pipeline finalizado: %d/%d arquivos OK "
            "(%d lotes), %d linhas inseridas no total",
            contagem["n_sucesso"],
            contagem["n_arquivos_total"],
            contagem["n_lotes"],
            contagem["total_linhas_inseridas"],
        )

        return contagem

    lotes = listar_arquivos_s3()
    resultados = baixar_e_extrair.expand(lote_de_arquivos=lotes)
    fechar_pipeline(resultados)


minc_extracao_anexos_dag()
