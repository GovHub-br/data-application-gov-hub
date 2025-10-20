import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from airflow.models.dag import DAG
from airflow.decorators import dag, task

API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

RAW_LAYER_PATH = Path("/opt/airflow/data/raw/camara_deputados")

default_args = {
    "owner": "Marcus",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="deputados_ingest_dag",
    description="DAG para ingestão diária dos dados de deputados.",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 20),
    catchup=False,
    default_args=default_args,
    tags=["deputados", "camara_deputados", "raw"],
)
def ingest_camara_deputados_deputados_dag() -> DAG:  # type: ignore[return]
    """
    ### DAG de Ingestão de Deputados da Câmara

    Esta DAG orquestra a extração de dados completos sobre os deputados da Câmara dos
    Deputados, abrangendo tanto o histórico quanto as atualizações diárias.

    1.  `get_legislature_ids`: Obtém os IDs de todas as legislaturas existentes.
    2.  `extract_deputies_by_legislature`: Para cada legislatura, extrai a lista completa
        de deputados, lidando com a paginação da API. Esta tarefa é executada
        dinamicamente para cada legislatura.
    3.  `save_deputies_to_raw`: Consolida os dados de todas as legislaturas em um único
        arquivo JSON e o armazena na camada raw do Data Lake, particionado pela data de
        execução.
    """

    @task
    def get_legislature_ids() -> List[int]:
        """
        Obtém os IDs de todas as legislaturas da API da Câmara.
        """
        legislaturas_url = f"{API_BASE_URL}/legislaturas"
        logging.info(f"Buscando IDs de legislaturas em: {legislaturas_url}")

        try:
            response = requests.get(legislaturas_url, timeout=30)
            response.raise_for_status()  # Lança exceção para códigos de erro HTTP
            data = response.json()

            ids = [legislatura["id"] for legislatura in data["dados"]]
            logging.info(f"Sucesso! {len(ids)} legislaturas encontradas.")
            return ids
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao buscar legislaturas: {e}")
            raise

    @task
    def extract_deputies_by_legislature(id_legislatura: int) -> List[Dict[str, Any]]:
        """
        Extrai todos os deputados para uma legislatura específica,
        lidando com a paginação.
        """
        deputados_da_legislatura = []
        next_url: Optional[str] = (
            f"{API_BASE_URL}/deputados?idLegislatura={id_legislatura}"
            "&itens=100&ordem=ASC"
        )

        logging.info(f"Iniciando extração para a legislatura ID: {id_legislatura}")

        page_count = 1
        while next_url:
            try:
                logging.info(
                    f"Processando página {page_count} da legislatura {id_legislatura}..."
                )
                response = requests.get(next_url, timeout=60)
                response.raise_for_status()
                data = response.json()

                deputados_da_legislatura.extend(data["dados"])

                next_url = None
                for link in data["links"]:
                    if link["rel"] == "next":
                        next_url = link["href"]
                        break
                page_count += 1

            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Erro ao buscar página para legislatura {id_legislatura}: {e}"
                )
                raise

        logging.info(
            f"Extração concluída para a legislatura {id_legislatura}. "
            f"Total de {len(deputados_da_legislatura)} deputados encontrados."
        )
        return deputados_da_legislatura

    @task
    def save_deputies_to_raw(
        listas_de_deputados: List[List[Dict]], execution_date: str
    ) -> str:
        """
        Consolida os dados de todas as legislaturas e salva
        em um único arquivo JSON na camada raw.
        """
        logging.info("Iniciando consolidação dos dados de todas as legislaturas.")

        todos_os_deputados = [
            deputado for sublist in listas_de_deputados for deputado in sublist
        ]

        unique_deputados = list({dep["id"]: dep for dep in todos_os_deputados}.values())

        logging.info(f"Total de deputados extraídos: {len(todos_os_deputados)}")
        logging.info(f"Total de deputados únicos a serem salvos: {len(unique_deputados)}")

        output_path = RAW_LAYER_PATH / "deputados" / f"extraction_date={execution_date}"
        output_path.mkdir(parents=True, exist_ok=True)

        output_file = output_path / "deputados.json"

        logging.info(f"Salvando dados consolidados em: {output_file}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(unique_deputados, f, ensure_ascii=False, indent=2)

        logging.info("Dados salvos com sucesso na camada raw.")
        return str(output_file)

    legislature_ids = get_legislature_ids()
    deputies_by_legislature_tasks = extract_deputies_by_legislature.expand(
        id_legislatura=legislature_ids
    )
    save_deputies_to_raw(
        listas_de_deputados=deputies_by_legislature_tasks, execution_date="{{ ds }}"
    )


ingest_camara_deputados_deputados_dag()
