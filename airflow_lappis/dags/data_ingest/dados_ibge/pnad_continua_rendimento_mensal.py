import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("pnad_continua_rendimento_mensal"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ibge", "pnad_continua", "rendimento", "cidades"],
)
def pnad_continua_rendimento_mensal() -> None:
    """DAG para buscar e armazenar o rendimento médio mensal real (PNAD Contínua - IBGE)."""

    @task
    def fetch_and_store_rendimento() -> None:
        logging.info("[pnad_continua_rendimento_mensal.py] Iniciando extração do rendimento médio mensal (IBGE)")

        api = ClienteIBGE()
        db = ClientPostgresDB(get_postgres_conn())

        ibge_data = api.get_rendimento_medio_mensal_real_anual_por_trimestre()

        if ibge_data:
            records_by_trimestre = {}
            for item in ibge_data:
                for resultado in item.get("resultados", []):
                    classificacoes = resultado.get("classificacoes", [])
                    if not classificacoes:
                        continue
                    
                    categoria = classificacoes[0].get("categoria", {})
                    if "47946" in categoria:
                        cat_name = "total"
                    elif "47949" in categoria:
                        cat_name = "construcao"
                    else:
                        continue

                    series = resultado.get("series", [])
                    if not series:
                        continue
                    
                    serie_values = series[0].get("serie", {})
                    for trim, val in serie_values.items():
                        if val == "-" or val == "..." or val == "X" or val is None or str(val).strip() == "":
                           val_num = None
                        else:
                           try:
                               val_num = round(float(val), 2)
                           except ValueError:
                               val_num = None

                        if trim not in records_by_trimestre:
                            records_by_trimestre[trim] = {"trimestre": trim, "total": None, "construcao": None}
                        records_by_trimestre[trim][cat_name] = val_num
            
            final_data = list(records_by_trimestre.values())
            for record in final_data:
                record["dt_ingest"] = datetime.now().isoformat()

            logging.info(
                f"[pnad_continua_rendimento_mensal.py] Inserindo "
                f"{len(final_data)} registros no schema dados_ibge"
            )

            db.insert_data(
                final_data,
                "pnad_continua_rendimento_mensal",
                conflict_fields=["trimestre"],
                primary_key=["trimestre"],
                schema="dados_ibge",
            )

            logging.info(
                f"[pnad_continua_rendimento_mensal.py] Concluído. "
                f"Total de {len(final_data)} registros processados."
            )
        else:
            logging.warning("[pnad_continua_rendimento_mensal.py] Nenhum dado de rendimento encontrado")

    fetch_and_store_rendimento()


pnad_continua_rendimento_mensal()
