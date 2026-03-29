{{ config(materialized='table') }}

WITH
bronze_deputados AS (
    SELECT * FROM {{ ref('deputados') }}
),

bronze_senadores AS (
    SELECT * FROM {{ ref('senadores') }}
),

sigla_map AS (
    SELECT
        TRIM(UPPER(sigla_origem)) AS sigla_origem,
        TRIM(UPPER(sigla_canonica)) AS sigla_canonica
    FROM {{ ref('partidos_map') }}
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

parlamentares_padronizados AS (
    SELECT
        p.*,
        COALESCE(m.sigla_canonica, p.sigla_partido) AS sigla_partido_padronizada
    FROM parlamentares_unificados p
    LEFT JOIN sigla_map m
        ON TRIM(UPPER(p.sigla_partido)) = m.sigla_origem
),

partidos_logo AS (
    SELECT
        TRIM(UPPER(sigla)) AS chave_join_sigla_partido,
        logo_url
    FROM {{ ref('partidos_logo') }}
)

SELECT
    p.id_parlamentar,
    p.chave_join_nome,
    p.nome_parlamentar,
    p.cargo_parlamentar,
    
    p.sigla_partido_padronizada AS sigla_partido,
    
    p.uf_parlamentar,
    p.url_foto,
    p.email,
    pl.logo_url AS url_logo_partido

FROM parlamentares_padronizados p

LEFT JOIN partidos_logo pl
  ON TRIM(UPPER(p.sigla_partido_padronizada)) = pl.chave_join_sigla_partido