{{ config(
    materialized='view'
) }}

SELECT
    identificador_unico,
    ja_acessou_recursos_bruto,
    'Pessoa Física' AS tipo_proponente
FROM {{ ref('agentes_pf') }}

UNION ALL

SELECT
    identificador_unico,
    ja_acessou_recursos_bruto,
    'Pessoa Jurídica' AS tipo_proponente
FROM {{ ref('agentes_pj') }}

UNION ALL

SELECT
    identificador_unico,
    ja_acessou_recursos_bruto,
    'Coletivo' AS tipo_proponente
FROM {{ ref('agentes_coletivos') }}
