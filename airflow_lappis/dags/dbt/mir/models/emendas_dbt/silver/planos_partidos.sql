{{ config(materialized='table') }}

WITH bronze_planos_acoes AS (
    SELECT * FROM {{ ref('planos_acoes') }} 
),

bronze_deputados AS (
    SELECT * FROM {{ ref('deputados') }}
),

bronze_senadores AS (
    SELECT * FROM {{ ref('senadores') }}
),

parlamentares_unificados AS (
    SELECT 
        id AS id_parlamentar,
        TRIM(UPPER(nome)) AS chave_join_nome, 
        nome AS nome_parlamentar,
        'Deputado' AS cargo_parlamentar,
        siglapartido AS sigla_partido,
        siglauf AS uf_parlamentar,
        urlfoto AS url_foto,
        email
    FROM bronze_deputados

    UNION ALL

    SELECT 
        id AS id_parlamentar,
        TRIM(UPPER(nome_parlamentar)) AS chave_join_nome, 
        nome_parlamentar AS nome_parlamentar,
        'Senador' AS cargo_parlamentar,
        sigla_partido AS sigla_partido,
        uf AS uf_parlamentar,
        url_foto AS url_foto,
        email
    FROM bronze_senadores
),

planos_acoes_tratado AS (
    SELECT 
        *,
        TRIM(UPPER(nome_parlamentar_emenda_plano_acao)) AS chave_join_nome
    FROM bronze_planos_acoes
),

final AS (
    SELECT 
        -- Todas as features de Planos de Ação
        pa.id_plano_acao,
        pa.codigo_plano_acao,
        pa.ano_plano_acao,
        pa.modalidade_plano_acao,
        pa.situacao_plano_acao,
        pa.cnpj_beneficiario_plano_acao,
        pa.nome_beneficiario_plano_acao,
        pa.uf_beneficiario_plano_acao,
        pa.codigo_banco_plano_acao,
        pa.codigo_situacao_dado_bancario_plano_acao,
        pa.nome_banco_plano_acao,
        pa.numero_agencia_plano_acao,
        pa.dv_agencia_plano_acao,
        pa.numero_conta_plano_acao,
        pa.dv_conta_plano_acao,
        pa.nome_parlamentar_emenda_plano_acao,
        pa.ano_emenda_parlamentar_plano_acao,
        pa.codigo_parlamentar_emenda_plano_acao,
        pa.sequencial_emenda_parlamentar_plano_acao,
        pa.numero_emenda_parlamentar_plano_acao,
        pa.codigo_emenda_parlamentar_formatado_plano_acao,
        pa.codigo_descricao_areas_politicas_publicas_plano_acao,
        pa.descricao_programacao_orcamentaria_plano_acao,
        pa.motivo_impedimento_plano_acao,
        pa.valor_custeio_plano_acao,
        pa.valor_investimento_plano_acao,
        pa.id_programa,
        
        -- Features unificadas dos Parlamentares
        parl.id_parlamentar,
        parl.cargo_parlamentar,
        parl.nome_parlamentar,
        parl.sigla_partido,
        parl.uf_parlamentar,
        parl.url_foto,
        parl.email,
        
        -- Data de ingestão (mantendo a da tabela fato)
        pa.dt_ingest
        
    FROM planos_acoes_tratado pa
    LEFT JOIN parlamentares_unificados parl
        ON pa.chave_join_nome = parl.chave_join_nome
)

SELECT * FROM final