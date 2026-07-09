"""Configuracoes compartilhadas do pipeline BSC/PNAB (extracao BB Agil e
consulta de beneficiarios via BSC/SERPRO).

Segue o mesmo padrao de configuracao via variavel de ambiente usado em
``cliente_siafi.py``/``cliente_siape.py`` (``os.getenv``), com fallback via
Airflow ``Variable`` para os parametros de negocio que podem mudar sem
precisar de deploy (mesmo padrao de ``schedule_loader.get_dynamic_schedule``).
"""

import os
from pathlib import Path

from airflow.sdk import Variable

# --------------------------------------------------------------------------
# Paths locais
# --------------------------------------------------------------------------
# Diretorio raiz de dados do pipeline. Em producao (docker-compose) resolve
# para /opt/airflow/data (volume montado); em execucao local resolve para
# <repo>/data.
DATA_DIR = Path(
    os.getenv("BSC_PNAB_DATA_DIR", str(Path(os.getenv("AIRFLOW_HOME", ".")) / "data"))
)

BBAGIL_DIR = DATA_DIR / "bbagil"
BBAGIL_EXTRATO_DIR = BBAGIL_DIR / "extrato"
BBAGIL_SUBTRANSACOES_DIR = BBAGIL_DIR / "subtransacoes"
BBAGIL_CONSOLIDADO_DIR = BBAGIL_DIR / "consolidado"

BENEFICIARIOS_DIR = DATA_DIR / "beneficiarios"
BPC_DIR = BENEFICIARIOS_DIR / "bpc"
CADUNICO_DIR = BENEFICIARIOS_DIR / "cadunico"
CNPJ_DIR = BENEFICIARIOS_DIR / "cnpj"
CPF_LIST_DIR = BENEFICIARIOS_DIR / "cpf"
RELACAO_TRABALHISTA_DIR = BENEFICIARIOS_DIR / "relacao_trabalhista"

FATO_BBAGIL_PATH = BBAGIL_CONSOLIDADO_DIR / "fato_bbagil.parquet"

# --------------------------------------------------------------------------
# Transferegov (descoberta oficial de agencia/conta dos entes -- substitui a
# planilha Excel legada que causava falha silenciosa em ambientes sem o
# arquivo mapeado)
# --------------------------------------------------------------------------
TRANSFEREGOV_LOG_DIR = Path(
    os.getenv("TRANSFEREGOV_LOG_DIR", str(DATA_DIR / "transferegov" / "logs"))
)

# --------------------------------------------------------------------------
# SCA / autenticacao
# --------------------------------------------------------------------------
SCA_TOKEN_URL = os.getenv("SCA_TOKEN_URL", "")
SCA_CLIENT_ID = os.getenv("SCA_CLIENT_ID", "")
SCA_CLIENT_SECRET = os.getenv("SCA_CLIENT_SECRET", "")
SCA_TOKEN_TTL_SECONDS = int(os.getenv("SCA_TOKEN_TTL_SECONDS", str(55 * 60)))

# --------------------------------------------------------------------------
# BSC / auditoria (campos obrigatorios em todo payload)
# --------------------------------------------------------------------------
# Dominio raiz do BSC. A autenticacao (SCA_TOKEN_URL) e os endpoints de
# negocio (SERPRO_BASE_URL) vivem sob prefixos distintos (/sca/ vs /serpro/)
# do mesmo dominio -- nao reaproveitar a base do token para as chamadas de
# negocio.
BSC_BASE_URL = os.getenv("BSC_BASE_URL", "https://bsc.cultura.gov.br")

# Base URL dos endpoints de negocio (BB Gestao Agil, CPF/CNPJ, CadUnico etc.),
# sob o prefixo /serpro/ conforme o Swagger do BSC.
SERPRO_BASE_URL = os.getenv("SERPRO_BASE_URL", f"{BSC_BASE_URL}/serpro")
BSC_CPF_CONSULTA = os.getenv("BSC_CPF_CONSULTA", "")
BSC_USUARIO = os.getenv("BSC_USUARIO", "admin.user")
BSC_IP_ORIGEM = os.getenv("BSC_IP_ORIGEM", "127.0.0.1")
BSC_IP_USUARIO = os.getenv("BSC_IP_USUARIO", "127.0.0.1")
BSC_TIMEOUT = int(os.getenv("BSC_TIMEOUT", "30"))

BSC_CADUNICO_AUTHORIZATION_ID = os.getenv("BSC_CADUNICO_AUTHORIZATION_ID", "")
BSC_CADUNICO_AUTHORIZATION_ID_TYPE = os.getenv(
    "BSC_CADUNICO_AUTHORIZATION_ID_TYPE", "Processo"
)
BSC_CADUNICO_CONSUMER_ID = os.getenv("BSC_CADUNICO_CONSUMER_ID", "MinC")
BSC_CADUNICO_CONSUMER_ID_TYPE = os.getenv("BSC_CADUNICO_CONSUMER_ID_TYPE", "Sigla")

# --------------------------------------------------------------------------
# Resiliencia (semaforo + throttle + retry)
# --------------------------------------------------------------------------
BSC_MAX_CONCURRENT_REQUESTS = int(os.getenv("BSC_MAX_CONCURRENT_REQUESTS", "5"))
BSC_REQUEST_THROTTLE_SECONDS = float(
    os.getenv("BSC_REQUEST_THROTTLE_SECONDS", os.getenv("BSC_SLEEP_SECONDS", "1.05"))
)
BSC_MAX_RETRIES = int(os.getenv("BSC_MAX_RETRIES", "3"))
BSC_RETRY_BACKOFF_BASE_SECONDS = float(os.getenv("BSC_RETRY_BACKOFF_BASE_SECONDS", "1.0"))
BSC_MAX_REQUESTS = int(os.getenv("BSC_MAX_REQUESTS", "200000"))

EMPTY_EXTRATO_ERROR_MESSAGE = "Não existem lançamentos para a conta no período informado"

# --------------------------------------------------------------------------
# Parametros de negocio (PNAB) - ajustaveis via Airflow Variable sem deploy
# --------------------------------------------------------------------------
LISTA_ANOS = Variable.get(
    "bsc_pnab_lista_anos", default=[2023, 2024, 2025, 2026], deserialize_json=True
)
LIMIAR_VALOR_BBAGIL = float(Variable.get("bsc_pnab_limiar_valor_bbagil", default="375"))
DATA_CORTE_PNAB = Variable.get("bsc_pnab_data_corte", default="2025-12-31")

# CNPJ do Fundo Nacional de Cultura (devolucoes de saldo, nao sao pagamentos)
FNC_CNPJ = os.getenv("BSC_FNC_CNPJ", "3793086100189")
# Codigo do proprio Banco do Brasil (tarifas/transferencias internas)
BB_BENEFICIARY_DOCUMENT_ID = os.getenv("BSC_BB_BENEFICIARY_DOCUMENT_ID", "191")
