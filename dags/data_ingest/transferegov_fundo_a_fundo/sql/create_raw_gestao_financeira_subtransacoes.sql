-- DDL de referencia para transferegov_fundo_a_fundo.raw_gestao_financeira_subtransacoes
--
-- IMPORTANTE: este arquivo e apenas documentacao/apoio para review e DBA.
-- A tabela real e criada e evoluida em runtime por
-- plugins/cliente_postgres.py::ClientPostgresDB (create_table_if_not_exists +
-- _evolve_schema), chamada pela DAG api_movimentacoes_financeiras_dag.py.
-- Todas as colunas da camada raw sao TEXT, incluindo IDs e valores
-- numericos, para blindar o pipeline contra Schema Drift da API do
-- Transferegov.
--
-- Colunas conforme documentacao Swagger oficial do endpoint
-- /gestao_financeira_subtransacoes.
--
-- NOTA: "numero_documento_beneficiario_subtransacao_gestao_financeira_ma" e
-- "descricao_tipo_pessoa_beneficiario_subtransacao_gestao_financei" sao
-- nomes truncados em 63 caracteres (limite padrao de identificador do
-- PostgreSQL/NAMEDATALEN) — mantidos EXATAMENTE como documentados no
-- Swagger, sem tentar "corrigir" o nome.
--
-- id_lancamento_gestao_financeira e a FK confirmada para
-- raw_gestao_financeira_lancamentos.

CREATE SCHEMA IF NOT EXISTS transferegov_fundo_a_fundo;

CREATE TABLE IF NOT EXISTS transferegov_fundo_a_fundo.raw_gestao_financeira_subtransacoes (
    "id_subtransacao_gestao_financeira" TEXT,
    "estado_subtransacao_gestao_financeira" TEXT,
    "situacao_pagamento_subtransacao_gestao_financeira" TEXT,
    "descricao_situacao_pagamento_subtransacao_gestao_financeira" TEXT,
    "data_pagamento_subtransacao_gestao_financeira" TEXT,
    "tipo_pessoa_beneficiario_subtransacao_gestao_financeira" TEXT,
    "descricao_tipo_pessoa_beneficiario_subtransacao_gestao_financei" TEXT,
    "numero_documento_beneficiario_subtransacao_gestao_financeira_ma" TEXT,
    "nome_beneficiario_subtransacao_gestao_financeira" TEXT,
    "codigo_banco_beneficiario_subtransacao_gestao_financeira" TEXT,
    "codigo_agencia_beneficiario_subtransacao_gestao_financeira" TEXT,
    "codigo_conta_beneficiario_subtransacao_gestao_financeira" TEXT,
    "descricao_subtransacao_gestao_financeira" TEXT,
    "valor_subtransacao_gestao_financeira" TEXT,
    "id_categoria_despesa_gestao_financeira" TEXT,
    "id_lancamento_gestao_financeira" TEXT,
    "dt_ingest" TEXT,
    PRIMARY KEY ("id_subtransacao_gestao_financeira")
);
