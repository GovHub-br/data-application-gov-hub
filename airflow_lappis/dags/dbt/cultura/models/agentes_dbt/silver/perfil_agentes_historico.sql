{{ config(
    materialized='table'
) }}

WITH identificadores AS (
    SELECT
        identificador_unico,
        tipo_proponente,
        programa_fomento,
        historico_acesso_bruto
    FROM {{ ref('identificadores_agentes') }}
),

higienizados AS (
    SELECT
        identificador_unico,
        tipo_proponente,
        programa_fomento,
        TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        historico_acesso_bruto,
                        '.', ''
                    ),
                    ';', ''
                ),
                '"', ''
            )
        ) AS historico_acesso_limpo
    FROM identificadores
    WHERE historico_acesso_bruto IS NOT NULL
      AND LOWER(historico_acesso_bruto) != 'nan'
      AND TRIM(historico_acesso_bruto) != ''
)

SELECT
    identificador_unico,
    tipo_proponente,
    programa_fomento,
    historico_acesso_limpo,
    CASE
        WHEN historico_acesso_limpo = 'sim' THEN 'Sim'
        WHEN historico_acesso_limpo IN ('não', 'nao', 'nâo') THEN 'Não'
        WHEN historico_acesso_limpo IN (
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
