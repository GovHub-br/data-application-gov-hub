-- DDL de referencia para transferegov_fundo_a_fundo.raw_gestao_financeira_lancamentos
--
-- IMPORTANTE: este arquivo e apenas documentacao/apoio para review e DBA.
-- A tabela real e criada e evoluida em runtime por
-- plugins/cliente_postgres.py::ClientPostgresDB (create_table_if_not_exists +
-- _evolve_schema), chamada pela DAG api_movimentacoes_financeiras_dag.py.
-- Todas as colunas da camada raw sao TEXT, incluindo IDs e valores
-- numericos, para blindar o pipeline contra Schema Drift da API do
-- Transferegov (colunas novas retornadas pela API sao adicionadas
-- automaticamente via ALTER TABLE, sem quebrar a ingestao).
--
-- Colunas conforme documentacao Swagger oficial do endpoint
-- /gestao_financeira_lancamentos.
--
-- NOTA CRITICA: este endpoint NAO possui campo id_plano_acao no payload.
-- O cruzamento com transferegov_fundo_a_fundo.raw_planos_acao deve ser
-- feito posteriormente (fora desta tabela raw) via
-- cnpj_ente_solicitante_gestao_financeira ou pelo endpoint-ponte
-- /plano_acao_dado_bancario.

CREATE SCHEMA IF NOT EXISTS transferegov_fundo_a_fundo;

CREATE TABLE IF NOT EXISTS transferegov_fundo_a_fundo.raw_gestao_financeira_lancamentos (
    "id_lancamento_gestao_financeira" TEXT,
    "origem_solicitacao_gestao_financeira" TEXT,
    "descricao_origem_solicitacao_gestao_financeira" TEXT,
    "cnpj_ente_solicitante_gestao_financeira" TEXT,
    "nome_ente_solicitante_gestao_financeira" TEXT,
    "nome_personalizado_ente_solicitante_gestao_financeira" TEXT,
    "codigo_programa_agil_ente_solicitante_gestao_financeira" TEXT,
    "codigo_banco_gestao_financeira" TEXT,
    "codigo_agencia_gestao_financeira" TEXT,
    "codigo_conta_gestao_financeira" TEXT,
    "data_lancamento_gestao_financeira" TEXT,
    "valor_lancamento_gestao_financeira" TEXT,
    "quantidade_subtransacoes_lancamento_gestao_financeira" TEXT,
    "id_agencia_conta" TEXT,
    "dt_ingest" TEXT,
    PRIMARY KEY ("id_lancamento_gestao_financeira")
);
