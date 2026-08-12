import gc
import hashlib
import io
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import TriggerRule

import schemas_minc as schemas
from cliente_postgres import ClientPostgresDB
from extracao_planilhas import (
    TimeoutLeituraError,
    extrair_lpg,
    extrair_pnab,
    resolver_tabela_planilha,
)
from postgres_helpers import get_postgres_conn
from views_compatibilidade import criar_views_compatibilidade

# Politicas com tabela de planilha definida no documento (secao 7.3). Um
# anexo de programa fora desta lista -- PNAB Ciclo 2, por exemplo -- nao tem
# tabela destino e por isso e pulado, em vez de criar tabela nova.
_PROGRAMA_POR_POLITICA = {
    "LPG": schemas.IDS_PROGRAMA_LPG,
    "PNAB": schemas.IDS_PROGRAMA_PNAB_CICLO_1,
}

_URL_ANEXO_RG = (
    "https://fundos.transferegov.sistema.gov.br/"
    "maisbrasil-transferencia-backend/api/public/anexos/rg/"
)

# Teto de colunas das tabelas de planilha. Elas consolidam planilhas de
# milhares de entes, cada uma com seus proprios cabecalhos: sem teto, o
# numero de colunas cresce sem limite ate o Postgres recusar em 1600 e
# quebrar a carga para todo mundo. Passando daqui, coluna nova vai para
# payload_origem -- nada se perde, mas a tabela para de crescer.
_LIMITE_COLUNAS_PLANILHA = 1200

_S3_CONN_ID = "minio_default"

default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

_CHUNK_SIZE = 50
_MAX_FILE_BYTES = 10 * 1024 * 1024  # 10 MB — limite para evitar OOM no Worker


def _hash_registro(id_anexo: str, indice_subtabela: int, linha_origem: int) -> str:
    """Chave determinística de uma linha de planilha (seção 9.1).

    Um mesmo anexo pode render várias subtabelas da mesma aba (a PNAB quebra
    uma aba em blocos por tipo de edital), então aba + linha não bastam para
    identificar o registro — o índice da subtabela desempata. Como a extração
    é determinística, reprocessar o anexo gera os mesmos hashes e o UPSERT
    atualiza em vez de duplicar.
    """
    return hashlib.md5(
        f"{id_anexo}|{indice_subtabela}|{linha_origem}".encode()
    ).hexdigest()


@dag(
    dag_id="extracao_anexos_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "anexos", "planilhas", "raw"],
)
def extracao_anexos_dag() -> None:
    """DAG de extração de tabelas de anexos (XLSX/XLS/XLSM) para os
    programas LPG e PNAB, com Dynamic Task Mapping por **lote** de arquivos.

    Fluxo:

    1. ``listar_anexos_pendentes`` — monta a lista a partir do **Postgres**,
       não do MinIO: o join ``anexos_relatorios → relatorios_gestao →
       plano_acao_minc`` traz, junto do caminho do arquivo, as chaves que a
       seção 7.3 exige em cada linha de planilha (``id_relatorio_gestao``,
       ``id_plano_acao``, ``id_programa``, ``cod_ibge``). Varrer as keys do
       MinIO daria o arquivo, mas não diria de que plano de ação ele é.
       A lista sai fatiada em blocos (chunks) de 50 arquivos.
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
    def listar_anexos_pendentes() -> list[list[dict[str, Any]]]:
        """Monta a lista de anexos a processar a partir do Postgres e a fatia
        em blocos (chunks) de 50 arquivos.

        Retorna ``list[list[dict]]`` para que o Dynamic Task Mapping crie
        uma task por *lote* ao invés de uma task por *arquivo*, reduzindo
        ~10.000 tasks para ~200 e evitando OOM/timeout no Worker.

        NOTA: extensão .ods é excluída propositalmente — a engine odf
        carrega o DOM XML inteiro em memória e causa OOM Kills.
        """
        db = ClientPostgresDB(get_postgres_conn())
        linhas = db.execute_query(
            "SELECT anexo.id, anexo.nome, anexo.caminho_minio, "
            "       anexo.id_relatorio_gestao, plano.id_plano_acao, "
            "       plano.id_programa, plano.cod_ibge "
            f"FROM {schemas.SCHEMA_TRANSFEREGOV}."
            f"{schemas.TABELA_ANEXO_RELATORIO} anexo "
            f"JOIN {schemas.SCHEMA_TRANSFEREGOV}."
            f"{schemas.TABELA_RELATORIO_GESTAO} relatorio "
            "  ON anexo.id_relatorio_gestao = relatorio.id_relatorio_gestao "
            f"JOIN {schemas.SCHEMA_TRANSFEREGOV}.{schemas.TABELA_PLANO_ACAO} plano "
            "  ON relatorio.id_plano_acao = plano.id_plano_acao "
            "WHERE anexo.caminho_minio IS NOT NULL"
        )

        # .ods removido intencionalmente — engine odf causa OOM
        extensoes_validas = {".xlsx", ".xls", ".xlsm", ".xlsb"}
        arquivos_meta: list[dict[str, Any]] = []
        sem_politica = 0
        extensao_ignorada = 0

        for (
            id_anexo,
            nome_arquivo,
            caminho_minio,
            id_relatorio_gestao,
            id_plano_acao,
            id_programa,
            cod_ibge,
        ) in linhas:
            # id_programa vem como TEXT do banco (toda coluna nasce TEXT na
            # camada raw) e pode faltar em plano mal cadastrado na origem —
            # nesse caso o anexo é da validação 12.8 (anexo sem programa
            # identificado) e não tem como ser roteado.
            try:
                id_programa_int = int(id_programa)
            except (TypeError, ValueError):
                id_programa_int = None

            nome_politica = next(
                (
                    politica
                    for politica, ids in _PROGRAMA_POR_POLITICA.items()
                    if id_programa_int in ids
                ),
                None,
            )
            if nome_politica is None:
                # O anexo tem plano de ação, mas o programa dele não tem
                # tabela de planilha definida no documento (PNAB Ciclo 2, por
                # exemplo) — ou nem programa identificado tem.
                sem_politica += 1
                continue

            # caminho_minio e "<bucket>/<key>", gravado por download_anexos_dag
            bucket, _, key = str(caminho_minio).partition("/")
            if not key:
                logging.warning(
                    "[extracao_anexos_dag.py] caminho_minio inválido no anexo "
                    "%s: %r — pulando",
                    id_anexo,
                    caminho_minio,
                )
                continue

            if Path(key).suffix.lower() not in extensoes_validas:
                extensao_ignorada += 1
                continue
            if Path(key).name.startswith("~$"):
                continue

            arquivos_meta.append({
                "key": key,
                "bucket": bucket,
                "nome_programa": nome_politica,
                "id_anexo": str(id_anexo),
                "id_relatorio_gestao": id_relatorio_gestao,
                "id_plano_acao": id_plano_acao,
                "id_programa": id_programa,
                "cod_ibge": cod_ibge,
                "nome_arquivo_origem": nome_arquivo,
                "url_origem": f"{_URL_ANEXO_RG}{id_anexo}",
            })

        logging.info(
            "[extracao_anexos_dag.py] %d anexos com arquivo baixado: %d a "
            "processar, %d de programas sem tabela de planilha, %d com "
            "extensão ignorada (.ods/.pdf/etc)",
            len(linhas),
            len(arquivos_meta),
            sem_politica,
            extensao_ignorada,
        )

        if not arquivos_meta:
            logging.warning(
                "[extracao_anexos_dag.py] Nenhum anexo pendente de extração"
            )
            return []

        # ── Chunking: fatia a lista em blocos de _CHUNK_SIZE ──
        chunks: list[list[dict[str, Any]]] = [
            arquivos_meta[i : i + _CHUNK_SIZE]
            for i in range(0, len(arquivos_meta), _CHUNK_SIZE)
        ]

        logging.info(
            "[extracao_anexos_dag.py] %d arquivos → %d lotes de até %d arquivos",
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

        O roteamento é explícito por política:
        - PNAB → extrair_pnab (roteamento por aba)
        - LPG → extrair_lpg (roteamento por template)

        Cada subtabela extraída cai numa das seis tabelas de
        ``relatorio_gestao`` (``resolver_tabela_planilha``), com as chaves e
        os metadados de origem que a seção 7.3 exige.

        Retorna metadados leves (sem dados_json) para o resumo do lote.
        Nunca levanta exceção — erros são capturados e devolvidos no dict.
        """
        nome_programa = file_meta["nome_programa"]
        bucket = file_meta["bucket"]
        key = file_meta["key"]
        file_name = Path(key).name

        # ── Checagem de tamanho + download em chamada S3 única ──
        try:
            obj = s3_hook.get_key(key=key, bucket_name=bucket)
            tamanho_bytes = obj.content_length
        except Exception as exc:
            logging.warning(
                "[extracao_anexos_dag.py] Não foi possível obter "
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
                "[extracao_anexos_dag.py] Arquivo '%s' ignorado — "
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
            "[extracao_anexos_dag.py] Baixando s3://%s/%s (%.2f MB) para memória",
            bucket,
            key,
            tamanho_mb,
        )

        # ── Download — reutiliza o obj já obtido (sem 2a chamada get_key) ──
        try:
            file_content = obj.get()["Body"].read()
        except Exception as exc:
            logging.error(
                "[extracao_anexos_dag.py] Erro ao baixar s3://%s/%s: %s",
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

        # hash_arquivo (secao 7.3): identifica o anexo pelo conteudo, o que
        # permite detectar o mesmo arquivo reenviado ou alterado (validacao
        # 12.9). Calculado aqui porque os bytes ja estao em memoria.
        hash_arquivo = hashlib.md5(file_content).hexdigest()

        buffer = io.BytesIO(file_content)
        # Libera referência ao conteúdo bruto logo após criar o buffer
        del file_content

        try:
            id_anexo = file_meta["id_anexo"]

            # ── Roteamento explícito por programa ──
            if nome_programa == "PNAB":
                # ── Fluxo PNAB: roteamento por aba ──
                logging.info(
                    "[extracao_anexos_dag.py] Extraindo PNAB '%s' "
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
                    "[extracao_anexos_dag.py] Extraindo LPG '%s' "
                    "com roteamento por template",
                    file_name,
                )
                resultados = extrair_lpg(
                    file_buffer=buffer,
                    file_name=file_name,
                    id_anexo=id_anexo,
                )

            else:
                # Nao ha outro caso: a listagem so devolve anexos de
                # programas de LPG ou PNAB Ciclo 1.
                raise ValueError(f"Política sem extrator: {nome_programa}")

            # ── Inserção comum para PNAB e LPG ──
            n_subtabelas = len(resultados)
            total_linhas = 0

            for indice_subtabela, res in enumerate(resultados):
                # ── try-except por subtabela: INSERT falho não
                # quebra as demais subtabelas do mesmo arquivo ──
                try:
                    df = res["dataframe"]
                    tabela_origem = res["nome_tabela_destino"]
                    tabela_destino = resolver_tabela_planilha(tabela_origem)

                    if tabela_destino is None:
                        logging.warning(
                            "[extracao_anexos_dag.py] %s '%s': subtabela '%s' "
                            "sem tabela destino no modelo — descartada",
                            nome_programa,
                            file_name,
                            tabela_origem,
                        )
                        continue

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
                            "[extracao_anexos_dag.py] Limpeza %s '%s' → %s: "
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
                            "[extracao_anexos_dag.py] %s '%s' → %s: "
                            "descartada por estar vazia após limpeza",
                            nome_programa,
                            file_name,
                            tabela_destino,
                        )
                        continue

                    # ── Colunas obrigatórias da seção 7.3 ──
                    # linha_origem é a posição da linha dentro da subtabela
                    # extraída, não a linha física do Excel: o parsing corta
                    # cabeçalhos, linhas de instrução e vazios antes daqui.
                    # Junto de id_anexo e do índice da subtabela, identifica
                    # o registro de forma estável entre execuções.
                    df["linha_origem"] = range(1, len(df) + 1)
                    df["hash_registro"] = [
                        _hash_registro(id_anexo, indice_subtabela, linha)
                        for linha in df["linha_origem"]
                    ]
                    df["id_anexo"] = id_anexo
                    df["id_relatorio_gestao"] = file_meta["id_relatorio_gestao"]
                    df["id_plano_acao"] = file_meta["id_plano_acao"]
                    df["id_programa"] = file_meta["id_programa"]
                    df["cod_ibge"] = file_meta["cod_ibge"]
                    df["url_origem"] = file_meta["url_origem"]
                    df["nome_arquivo_origem"] = file_meta["nome_arquivo_origem"]
                    df["hash_arquivo"] = hash_arquivo
                    df["aba_origem"] = res.get("aba")
                    df["tabela_origem"] = tabela_origem
                    df["indice_subtabela"] = indice_subtabela
                    df["nome_arquivo"] = file_name
                    df["nome_programa"] = nome_programa
                    df["dt_extracao"] = datetime.now().isoformat()
                    df["dt_ingest"] = df["dt_extracao"]

                    linhas = df.to_dict(orient="records")

                    logging.info(
                        "[extracao_anexos_dag.py] %s '%s' → %s (origem %s): "
                        "inserindo %d registros em %s.%s",
                        nome_programa,
                        file_name,
                        tabela_destino,
                        tabela_origem,
                        len(df),
                        schemas.SCHEMA_RELATORIO_GESTAO,
                        tabela_destino,
                    )
                    db.insert_data_por_tabela(
                        linhas,
                        table_name=tabela_destino,
                        schema=schemas.SCHEMA_RELATORIO_GESTAO,
                        # Chave natural da seção 9.1: reprocessar o mesmo
                        # anexo atualiza as linhas em vez de duplicá-las.
                        primary_key=["hash_registro"],
                        conflict_fields=["hash_registro"],
                        limite_colunas=_LIMITE_COLUNAS_PLANILHA,
                    )
                    total_linhas += len(linhas)
                except Exception as exc_sub:
                    logging.warning(
                        "[extracao_anexos_dag.py] %s '%s' → subtabela "
                        "'%s' falhou no INSERT: %s — continuando",
                        nome_programa,
                        file_name,
                        res.get("nome_tabela_destino", "?"),
                        exc_sub,
                    )
                    continue

            logging.info(
                "[extracao_anexos_dag.py] %s '%s': %d subtabelas, "
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
                "[extracao_anexos_dag.py] Arquivo '%s' ignorado por "
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
                "[extracao_anexos_dag.py] Arquivo ignorado por corrupção "
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
                    "[extracao_anexos_dag.py] Erro crítico no arquivo '%s': %s",
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
            "[extracao_anexos_dag.py] Lote finalizado: %d/%d OK, "
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
                "[extracao_anexos_dag.py] %d arquivos com erro de download, "
                "%d com erro de extração, %d com erro de tamanho, "
                "%d com erro crítico",
                contagem["n_erro_download"],
                contagem["n_erro_extracao"],
                contagem["n_erro_tamanho"],
                contagem["n_erro_critico"],
            )

        logging.info(
            "[extracao_anexos_dag.py] Pipeline finalizado: %d/%d arquivos OK "
            "(%d lotes), %d linhas inseridas no total",
            contagem["n_sucesso"],
            contagem["n_arquivos_total"],
            contagem["n_lotes"],
            contagem["total_linhas_inseridas"],
        )

        return contagem

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def publicar_views_compatibilidade() -> int:
        """(Re)cria as views com os nomes antigos de tabela, que os modelos
        dbt existentes ainda consomem.

        Fica aqui, no fim da cadeia do TransfereGov, porque a essa altura
        todas as tabelas novas já existem. Ver ``views_compatibilidade``.
        """
        return criar_views_compatibilidade(ClientPostgresDB(get_postgres_conn()))

    lotes = listar_anexos_pendentes()
    resultados = baixar_e_extrair.expand(lote_de_arquivos=lotes)
    fechar_pipeline(resultados) >> publicar_views_compatibilidade()


extracao_anexos_dag()
