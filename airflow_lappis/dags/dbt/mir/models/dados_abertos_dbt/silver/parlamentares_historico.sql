{{ config(materialized='table') }}

WITH
bronze_deputados_historico AS (
    SELECT * FROM {{ ref('deputados_historico') }}
),

bronze_deputados AS (
    SELECT * FROM {{ ref('deputados') }}
),

deputados_lookup AS (
    SELECT DISTINCT ON (id)
        id,
        nome,
        siglauf,
        urlfoto,
        email,
        dt_ingest
    FROM bronze_deputados
    ORDER BY id, dt_ingest DESC
),

bronze_senadores_historico AS (
    SELECT * FROM {{ ref('senadores_historico') }}
),

bronze_senadores AS (
    SELECT * FROM {{ ref('senadores') }}
),

senadores_lookup AS (
    SELECT DISTINCT ON (id)
        id,
        nome_parlamentar,
        uf,
        url_foto,
        email,
        dt_ingest
    FROM bronze_senadores
    ORDER BY id, dt_ingest DESC
),

sigla_map AS (
    SELECT
        TRIM(UPPER(sigla_origem)) AS sigla_origem,
        MAX(TRIM(UPPER(sigla_canonica))) AS sigla_canonica
    FROM {{ ref('partidos_map') }}
    GROUP BY 1
),

parlamentares_unificados AS (
    SELECT
        dh.id AS id_parlamentar,
        {{ name_formater("COALESCE(NULLIF(dh.nome, ''), d.nome)") }} AS chave_join_nome,
        COALESCE(NULLIF(dh.nome, ''), d.nome) AS nome_parlamentar,
        'Deputado' AS cargo_parlamentar,
        dh.sigla_partido AS sigla_partido,
        d.siglauf AS uf_parlamentar,
        d.urlfoto AS url_foto,
        d.email AS email,
        dh.data_filiacao,
        dh.data_desfiliacao,
        dh.id_legislatura,
        dh.situacao,
        NULL::text AS fonte,
        dh.dt_ingest
    FROM bronze_deputados_historico dh
    LEFT JOIN deputados_lookup d
        ON dh.id = d.id

    UNION ALL

    SELECT
        sh.parlamentar_id AS id_parlamentar,
        {{ name_formater("COALESCE(NULLIF(sh.nome, ''), s.nome_parlamentar)") }} AS chave_join_nome,
        COALESCE(NULLIF(sh.nome, ''), s.nome_parlamentar) AS nome_parlamentar,
        'Senador' AS cargo_parlamentar,
        sh.sigla_partido AS sigla_partido,
        s.uf AS uf_parlamentar,
        s.url_foto AS url_foto,
        s.email AS email,
        sh.data_filiacao,
        sh.data_desfiliação AS data_desfiliacao,
        NULL::integer AS id_legislatura,
        NULL::text AS situacao,
        sh.fonte,
        sh.dt_ingest
    FROM bronze_senadores_historico sh
    LEFT JOIN senadores_lookup s
        ON sh.parlamentar_id = s.id
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
        MAX(logo_url) AS logo_url
    FROM {{ ref('partidos_logo') }}
    GROUP BY 1
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
    p.data_filiacao,
    p.data_desfiliacao,
    p.id_legislatura,
    p.situacao,
    p.fonte,
    p.dt_ingest,
    pl.logo_url AS url_logo_partido

FROM parlamentares_padronizados p

LEFT JOIN partidos_logo pl
  ON TRIM(UPPER(p.sigla_partido_padronizada)) = pl.chave_join_sigla_partido