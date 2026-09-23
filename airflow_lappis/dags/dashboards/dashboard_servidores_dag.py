"""
DAG para gerar arquivo JSON com dados do dashboard de servidores.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.models import Variable

from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_github import ClienteGitHub

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações do GitHub
GITHUB_OWNER = "GovHub-br"
GITHUB_REPO = "gov-hub"
GITHUB_FILE_PATH = "docs/land/public/data/pessoas_visao_geral.json"
GITHUB_BRANCH = "main"


@dag(
    dag_id="dashboard_servidores_json",
    schedule_interval="0 6 * * *",  # Executa diariamente às 6h
    start_date=datetime(2025, 11, 16),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dashboard", "pessoas", "json"],
    description="Gera arquivo JSON com dados do dashboard de servidores",
)
def dashboard_servidores_dag() -> None:
    """
    DAG que gera arquivo JSON consolidado com dados do dashboard de servidores.
    """

    @task
    def generate_dashboard_json() -> Dict:
        """
        Gera dados do dashboard de servidores.

        Returns:
            Dicionário com os dados do dashboard
        """
        logger.info("Iniciando geração dos dados do dashboard")

        try:
            # Conectar ao banco de dados usando helper
            postgres_conn_str = get_postgres_conn()
            client = ClientPostgresDB(postgres_conn_str)
            logger.info("Conectado ao banco de dados com sucesso")

            # Buscar dados
            logger.info("Buscando KPIs...")
            kpis = client.get_dashboard_kpis()

            logger.info("Buscando distribuição por gênero...")
            genero = client.get_dashboard_genero()

            logger.info("Buscando distribuição por raça/cor...")
            raca_cor = client.get_dashboard_raca_cor()

            logger.info("Buscando distribuição por situação funcional...")
            situacao_funcional = client.get_dashboard_situacao_funcional()

            logger.info("Buscando distribuição geográfica por UF...")
            mapa_uf = client.get_dashboard_mapa_uf()

            logger.info("Buscando tabela de servidores agregada...")
            tabela_servidores = client.get_dashboard_tabela_servidores(limit=100)

            # Montar estrutura do JSON
            dashboard_data = {
                "meta": {"atualizado_em": datetime.now().isoformat() + "Z"},
                "kpis": {
                    "total_servidores": kpis.get("total_servidores", 0),
                    "servidores_ativos_permanentes": kpis.get(
                        "servidores_ativos_permanentes", 0
                    ),
                    "aposentados": kpis.get("aposentados", 0),
                    "estagiarios": kpis.get("estagiarios", 0),
                    "terceirizados": kpis.get("terceirizados", 0),
                },
                "genero": genero,
                "raca_cor": raca_cor,
                "mapa_uf": mapa_uf,
                "situacao_funcional": situacao_funcional,
                "tabela_servidores": tabela_servidores,
            }

            return dashboard_data

        except Exception as e:
            logger.error(f"Erro ao gerar dados do dashboard: {str(e)}")
            raise

    @task
    def publish_to_github(dashboard_data: Dict) -> Dict[str, str]:
        """
        Publica os dados do dashboard no repositório GitHub.

        Args:
            dashboard_data: Dicionário com os dados do dashboard

        Returns:
            Informações sobre o commit realizado
        """
        logger.info("Iniciando publicação dos dados no GitHub")

        try:
            # Obter token do GitHub das variáveis do Airflow
            github_token = Variable.get("GITHUB_TOKEN")
            logger.info("Token do GitHub obtido com sucesso")

            # Converter dicionário para JSON string
            json_content = json.dumps(dashboard_data, ensure_ascii=False, indent=2)
            logger.info("Dados convertidos para JSON")

            # Inicializar cliente GitHub
            github_client = ClienteGitHub(github_token)

            # Criar mensagem de commit
            commit_message = (
                f"Update dashboard data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Publicar no GitHub
            logger.info(f"Publicando em {GITHUB_OWNER}/{GITHUB_REPO}:{GITHUB_FILE_PATH}")
            result = github_client.update_file(
                owner=GITHUB_OWNER,
                repo=GITHUB_REPO,
                path=GITHUB_FILE_PATH,
                content=json_content,
                message=commit_message,
                branch=GITHUB_BRANCH,
            )

            commit_info = {
                "commit_sha": result.get("commit", {}).get("sha", ""),
                "commit_url": result.get("commit", {}).get("html_url", ""),
                "file_url": result.get("content", {}).get("html_url", ""),
            }

            logger.info("Arquivo publicado com sucesso no GitHub!")
            logger.info(f"Commit SHA: {commit_info['commit_sha']}")
            logger.info(f"URL do arquivo: {commit_info['file_url']}")

            return commit_info

        except Exception as e:
            logger.error(f"Erro ao publicar no GitHub: {str(e)}")
            raise

    # Definir dependências entre tasks usando XCom
    dashboard_data = generate_dashboard_json()
    publish_to_github(dashboard_data)


dag_instance = dashboard_servidores_dag()
