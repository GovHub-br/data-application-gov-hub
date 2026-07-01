{{ config(
    materialized='table'
) }}

WITH todos_proponentes AS (
    SELECT
        identificador_unico,
        tipo_proponente,
        programa_fomento,
        historico_acesso_bruto
    FROM {{ ref('identificadores_agentes') }}
),

historico_limpo AS (
    SELECT
        identificador_unico,
        tipo_proponente,
        programa_fomento,
        CASE
            WHEN historico_acesso_bruto IS NULL
              OR LOWER(TRIM(historico_acesso_bruto)) IN ('', 'nan')
            THEN NULL
            ELSE TRIM(
                    REPLACE(
                        REPLACE(
                            REPLACE(historico_acesso_bruto, '.', ''),
                            ';', ''
                        ),
                        '"', ''
                    )
                 )
        END AS historico_acesso_limpo
    FROM todos_proponentes
),

historico_ordenado AS (
    SELECT
        identificador_unico,
        tipo_proponente,
        programa_fomento,
        historico_acesso_limpo,
        ROW_NUMBER() OVER (
            PARTITION BY identificador_unico
            ORDER BY programa_fomento ASC
        ) AS sequencia_fomento
    FROM historico_limpo
)

SELECT
    identificador_unico,
    tipo_proponente,
    programa_fomento,
    historico_acesso_limpo,
    sequencia_fomento,
    CASE
        WHEN historico_acesso_limpo = 'sim'
            THEN 'Confirmado - Primeira Vez'
        WHEN historico_acesso_limpo IN ('não', 'nao', 'nâo')
            THEN 'Confirmado - Veterano'
        WHEN (
            historico_acesso_limpo IS NULL
            OR historico_acesso_limpo IN (
                'não sei informar', 'nao sei informar',
                'não informado',    'nao informado',
                'nao_declarar',
                'não sei',          'nao sei',
                'não sabe',         'nao sabe'
            )
        ) AND sequencia_fomento = 1
            THEN 'Inferido - Primeira Vez (Estreante na base)'
        WHEN (
            historico_acesso_limpo IS NULL
            OR historico_acesso_limpo IN (
                'não sei informar', 'nao sei informar',
                'não informado',    'nao informado',
                'nao_declarar',
                'não sei',          'nao sei',
                'não sabe',         'nao sabe'
            )
        ) AND sequencia_fomento > 1
            THEN 'Inferido - Veterano (Possui histórico)'
        ELSE 'Não sabe/Não informou'
    END AS perfil_acesso_fomento
FROM historico_ordenado
