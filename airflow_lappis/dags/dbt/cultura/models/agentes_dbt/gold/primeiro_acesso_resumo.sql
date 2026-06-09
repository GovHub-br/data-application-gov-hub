{{ config(
    materialized='table'
) }}

WITH resumo AS (
    SELECT
        categoria_primeiro_acesso,
        COUNT(DISTINCT identificador_unico) AS total_proponentes
    FROM {{ ref('perfil_agentes_historico') }}
    GROUP BY categoria_primeiro_acesso
)

SELECT
    categoria_primeiro_acesso,
    total_proponentes,
    ROUND(
        (total_proponentes * 100.0) / SUM(total_proponentes) OVER (),
        2
    ) AS percentual
FROM resumo
ORDER BY total_proponentes DESC
