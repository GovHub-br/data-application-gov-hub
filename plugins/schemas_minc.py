"""Nomes de schema e tabela do banco do MinC.

Fonte: "Especificacao do banco de dados dos programas do Ministerio da
Cultura" (secao 4). Ate aqui esses nomes eram literais repetidos em oito
arquivos, e foi assim que o codigo divergiu do documento --
``transferegov_fundo_a_fundo`` no lugar de ``transferegov``, ``bsc_pnab``
no lugar de ``bbagil``, planilhas com nome derivado do nome da aba do Excel.

Centralizar importa mais do que parece: ``ClientPostgresDB`` roda
``CREATE SCHEMA IF NOT EXISTS`` a cada insert, entao um literal errado nao
quebra a DAG -- ele cria silenciosamente um schema fora do padrao no banco
do MinC.

Este modulo e so constante: sem I/O, sem Airflow, sem regra de negocio.
"""

# ---------------------------------------------------------------------------
# Schemas (secao 4)
# ---------------------------------------------------------------------------
SCHEMA_TRANSFEREGOV = "transferegov"
SCHEMA_BBAGIL = "bbagil"
SCHEMA_RELATORIO_GESTAO = "relatorio_gestao"

# ---------------------------------------------------------------------------
# transferegov (secao 7.1)
# ---------------------------------------------------------------------------
TABELA_PROGRAMA = "programa_minc"
TABELA_PLANO_ACAO = "plano_acao_minc"
TABELA_PLANO_ACAO_META = "plano_acao_meta_minc"
TABELA_PLANO_ACAO_DADO_BANCARIO = "plano_acao_dado_bancario_minc"

# Apoio: o documento nao preve essas duas, mas elas sao o insumo da cadeia
# de anexos (plano de acao -> relatorio de gestao -> anexo -> planilha).
# Ficam no schema de quem as produz.
TABELA_RELATORIO_GESTAO = "relatorios_gestao"
TABELA_ANEXO_RELATORIO = "anexos_relatorios"

# ---------------------------------------------------------------------------
# bbagil (secao 7.2)
# ---------------------------------------------------------------------------
TABELA_EXTRATO = "extrato_bbagil"
TABELA_SUBTRANSACAO = "subtransacao_bbagil"

# Apoio: controle de retomada da extracao (o que ja foi tentado e com que
# resultado). Nao e dado de negocio, mas vive junto de quem controla.
TABELA_CONTROLE_EXTRATO = "controle_extracao_bbagil_extrato"
TABELA_CONTROLE_SUBTRANSACAO = "controle_extracao_bbagil_subtransacoes"

# ---------------------------------------------------------------------------
# relatorio_gestao (secao 7.3)
# ---------------------------------------------------------------------------
TABELA_PLANILHA_CONTEMPLADOS_LPG = "planilha_contemplados_lpg"
TABELA_PLANILHA_EDITAIS_LPG = "planilha_editais_lpg"
TABELA_PLANILHA_DADOS_LPG = "planilha_dados_lpg"
TABELA_PLANILHA_CONTEMPLADOS_PNAB = "planilha_contemplados_pnab_ciclo_1"
TABELA_PLANILHA_EDITAIS_PNAB = "planilha_editais_pnab_ciclo_1"
TABELA_PLANILHA_DADOS_PNAB = "planilha_dados_pnab_ciclo_1"

# ---------------------------------------------------------------------------
# Escopo de programas
# ---------------------------------------------------------------------------
# Os 11 programas das 4 politicas monitoradas (secao 2). E so o *default*:
# o valor efetivo vem da Variable ``transferegov_programas_ids``, ajustavel
# sem deploy. Estava desalinhado entre as DAGs ([46, 47] em umas,
# [46, 47, 60, 61, 62] em outras), o que deixava LAB1 e PNAB Ciclo 2 de fora
# sem ninguem perceber.
PROGRAMAS_IDS_PADRAO = [7, 8, 9, 15, 46, 47, 60, 61, 62, 111, 112]

# Recorte por politica, usado para rotear anexo -> tabela de planilha: o
# documento so define tabelas de planilha para LPG e PNAB Ciclo 1.
IDS_PROGRAMA_LPG = [46, 47]
IDS_PROGRAMA_PNAB_CICLO_1 = [60, 61, 62]
