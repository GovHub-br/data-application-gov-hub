import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_senadores import ClienteSenadores
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("senadores_ingest_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Ingrid",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["senado_federal", "senadores", "dados_abertos", "MIR"],
)
def senadores_ingest_dag() -> None:
    """DAG para buscar e armazenar dados de senadores do Senado Federal."""

    @task
    def fetch_and_store_senadores() -> None:
        logging.info("[senadores_ingest_dag.py] Iniciando extração de senadores")

        api = ClienteSenadores()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        senadores_data = api.get_senadores_por_legislatura()

        if senadores_data and len(senadores_data) > 0:
            registros_limpos = []

            for item in senadores_data:
                info = item.get("IdentificacaoParlamentar", {})
                mandato = item.get("Mandato", {})

                senador_simplificado = {
                    "id": info.get("CodigoParlamentar"),
                    "nome_parlamentar": info.get("NomeParlamentar"),
                    "nome_completo": info.get("NomeCompletoParlamentar"),
                    "sexo": info.get("SexoParlamentar"),
                    "forma_tratamento": info.get("FormaTratamento"),
                    "url_foto": info.get("UrlFotoParlamentar"),
                    "url_pagina": info.get("UrlPaginaParlamentar"),
                    "email": info.get("EmailParlamentar"),
                    "sigla_partido": info.get("SiglaPartidoParlamentar"),
                    "uf": info.get("UfParlamentar"),
                    "id_legislatura": mandato.get("NumeroLegislatura"),
                    "dt_ingest": datetime.now().isoformat(),
                }
                registros_limpos.append(senador_simplificado)

            logging.info(
                f"[senadores_ingest_dag.py] Inserindo {len(registros_limpos)} "
                f"senadores simplificados no schema dados_abertos, tabela senadores"
            )

            db.insert_data(
                registros_limpos,
                "senadores",
                conflict_fields=["id"],
                primary_key=["id"],
                schema="dados_abertos",
            )
    @task
    def fetch_and_store_filiacoes() -> None:
        api = ClienteSenadores()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        # 1. Busca a lista base de senadores
        senadores_base = api.get_senadores_por_legislatura()
        registros_filiacoes = []

        for sen in senadores_base:
            info = sen.get("IdentificacaoParlamentar", {})
            cod_id = info.get("CodigoParlamentar")
            nome = info.get("NomeParlamentar")

            if cod_id:
                # 2. Para cada senador, busca o histórico de filiações
                filiacoes = api.get_filiacoes_senador(cod_id)
                if isinstance(filiacoes, dict):
                    filiacoes = [filiacoes]
                elif not filiacoes:
                    logging.debug(
                        f"[senadores_ingest_dag.py] Nenhuma filiação encontrada para "
                        f"{nome} (ID: {cod_id})"
                    )
                    continue
                for f in filiacoes:
                    # Extrai os dados do objeto Partido aninhado
                    partido = f.get("Partido", {})
                    sigla_partido = partido.get("SiglaPartido") or "Sigla não disponível"
                    nome_partido = partido.get("NomePartido") or "Nome não disponível"
                    ano_filiacao = f.get("AnoFiliacao") or "Data não disponível"
                    ano_desfiliacao = f.get("AnoDesfiliacao")

                    registro = {
                        "id": cod_id,
                        "nome_parlamentar": nome,
                        "sigla_partido": sigla_partido,
                        "nome_partido": nome_partido,
                        "dt_filiacao": ano_filiacao,
                        "dt_desfiliacao": ano_desfiliacao,
                        "uf": info.get("UfParlamentar"),
                        "dt_ingest": datetime.now().isoformat(),
                    }
                    registros_filiacoes.append(registro)
                    logging.debug(
                        f"[senadores_ingest_dag.py] Filiação processada: "
                        f"{nome} -> {sigla_partido} ({ano_filiacao})"
                    )
        
        logging.info(
            f"[senadores_ingest_dag.py] Total de {len(registros_filiacoes)} "
            f"registros de filiações coletados da API."
        )

        # 3. Deduplicação baseada nas chaves únicas
        registros_deduplicated = {}
        for registro in registros_filiacoes:
            chave_unica = (
                registro.get("id"),
                registro.get("sigla_partido"),
                registro.get("dt_filiacao"),
            )
            if chave_unica not in registros_deduplicated:
                registros_deduplicated[chave_unica] = registro
        
        registros_filiacoes = list(registros_deduplicated.values())
        logging.info(
            f"[senadores_ingest_dag.py] Após deduplicação: {len(registros_filiacoes)} "
            f"registros únicos de histórico partidário."
        )

        # 4. Inserção no banco
        if registros_filiacoes:
            logging.info(f"Inserindo {len(registros_filiacoes)} registros de histórico partidário.")
            db.insert_data(
                registros_filiacoes,
                "senadores_filiacoes", # Nome da nova tabela única
                conflict_fields=["id", "sigla_partido", "dt_filiacao"],
                primary_key=["id", "sigla_partido", "dt_filiacao"], # Chave composta para permitir múltiplos registros do mesmo ID
                schema="senado_federal",
            )
    fetch_and_store_senadores()

    fetch_and_store_filiacoes()



senadores_ingest_dag()
