{{ config(
    materialized='table'
) }}

WITH resumo AS (
    SELECT
        programa_fomento,
        categoria_primeiro_acesso,
        COUNT(DISTINCT identificador_unico) AS total_proponentes
    FROM {{ ref('perfil_agentes_historico') }}
    GROUP BY programa_fomento, categoria_primeiro_acesso
)

SELECT
    programa_fomento,
    categoria_primeiro_acesso,
    total_proponentes,
    ROUND(
        (total_proponentes::NUMERIC
         / SUM(total_proponentes) OVER (PARTITION BY programa_fomento))
        * 100,
        2
    ) AS percentual
FROM resumo
ORDER BY programa_fomento, total_proponentes DESC
