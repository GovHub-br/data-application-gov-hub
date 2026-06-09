{{ config(
    materialized='table'
) }}

WITH identificadores AS (
    SELECT
        identificador_unico,
        tipo_proponente,
        ja_acessou_recursos_bruto
    FROM {{ ref('identificadores_agentes') }}
),

higienizados AS (
    SELECT
        identificador_unico,
        tipo_proponente,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        ja_acessou_recursos_bruto,
                        '.', ''
                    ),
                    ';', ''
                ),
                '"', ''
            )
        ) AS ja_acessou_recursos_limpo
    FROM identificadores
    WHERE ja_acessou_recursos_bruto IS NOT NULL
      AND LOWER(ja_acessou_recursos_bruto) != 'nan'
      AND TRIM(ja_acessou_recursos_bruto) != ''
)

SELECT
    identificador_unico,
    tipo_proponente,
    ja_acessou_recursos_limpo,
    CASE
        WHEN ja_acessou_recursos_limpo = 'sim' THEN 'Sim'
        WHEN ja_acessou_recursos_limpo IN ('não', 'nao', 'nâo') THEN 'Não'
        WHEN ja_acessou_recursos_limpo IN (
            'não sei informar',
            'nao sei informar',
            'não informado',
            'nao informado',
            'nao_declarar',
            'não sei',
            'nao sei',
            'não sabe',
            'nao sabe'
        ) THEN 'Não sabe/Não informou'
        ELSE 'Não sabe/Não informou'
    END AS categoria_primeiro_acesso
FROM higienizados
