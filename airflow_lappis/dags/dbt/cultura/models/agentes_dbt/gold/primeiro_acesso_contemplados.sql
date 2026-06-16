{{ config(materialized='table') }}

-- Normaliza identificadores para dígitos puro, eliminando diferenças de
-- formatação entre tabelas (CPF com pontos/traço vs. só dígitos, CNPJ com
-- pontos/barra vs. só dígitos).
-- Usada em ambos os lados do JOIN para garantir correspondência consistente.

WITH contemplados_lpg AS (
    SELECT DISTINCT
        REGEXP_REPLACE(LOWER(TRIM("cpf ou cnpj")), '[^0-9]', '', 'g') AS id_normalizado,
        'LPG' AS programa_fomento
    FROM {{ source('transferegov_fundo_a_fundo', 'lpg_contemplados') }}
    WHERE "cpf ou cnpj" IS NOT NULL
      AND LOWER(TRIM("cpf ou cnpj")) NOT IN ('nan', '', 'cpf ou cnpj')
      AND LENGTH(REGEXP_REPLACE(TRIM("cpf ou cnpj"), '[^0-9]', '', 'g')) >= 11
),

-- PNCV fornece CPF real dos contemplados PNAB (PF)
contemplados_pnab_pncv AS (
    SELECT DISTINCT
        REGEXP_REPLACE(TRIM(cpf), '[^0-9]', '', 'g') AS id_normalizado,
        'PNAB' AS programa_fomento
    FROM {{ source('transferegov_fundo_a_fundo', 'raw_pnab_lista_contemplados_pncv') }}
    WHERE cpf IS NOT NULL
      AND LOWER(TRIM(cpf)) NOT IN ('nan', '', 'cpf')
      AND LENGTH(REGEXP_REPLACE(TRIM(cpf), '[^0-9]', '', 'g')) = 11
),

-- Lista geral PNAB: CPF está anonimizado (***XXXXXX**) — só o CNPJ é utilizável
-- Cobertura parcial: PJ contemplada via PNAB geral sem registro no PNCV fica de fora
contemplados_pnab_geral_pj AS (
    SELECT DISTINCT
        REGEXP_REPLACE(TRIM(cnpj), '[^0-9]', '', 'g') AS id_normalizado,
        'PNAB' AS programa_fomento
    FROM {{ source('transferegov_fundo_a_fundo', 'raw_pnab_lista_contemplados_geral') }}
    WHERE cnpj IS NOT NULL
      AND LOWER(TRIM(cnpj)) NOT IN ('nan', '', 'cnpj')
      AND LENGTH(REGEXP_REPLACE(TRIM(cnpj), '[^0-9]', '', 'g')) = 14
),

todos_contemplados AS (
    SELECT id_normalizado, programa_fomento FROM contemplados_lpg
    UNION
    SELECT id_normalizado, programa_fomento FROM contemplados_pnab_pncv
    UNION
    SELECT id_normalizado, programa_fomento FROM contemplados_pnab_geral_pj
),

-- Base: perfil_acesso_fomento tem 1 linha por (identificador × programa),
-- preservando a granularidade por programa para o JOIN de contemplação
perfil_base AS (
    SELECT
        identificador_unico,
        programa_fomento,
        CASE
            WHEN perfil_acesso_fomento IN (
                'Confirmado - Primeira Vez',
                'Inferido - Primeira Vez (Estreante na base)'
            ) THEN 'Sim'
            WHEN perfil_acesso_fomento IN (
                'Confirmado - Veterano',
                'Inferido - Veterano (Possui histórico)'
            ) THEN 'Não'
            ELSE 'Não sabe/Não informou'
        END AS categoria_primeiro_acesso,
        CASE
            WHEN perfil_acesso_fomento LIKE 'Confirmado%' THEN 'Confirmado'
            WHEN perfil_acesso_fomento LIKE 'Inferido%'   THEN 'Inferido'
            ELSE 'Não Informado'
        END AS status_dado
    FROM {{ ref('perfil_acesso_fomento') }}
),

perfil_com_contemplado AS (
    SELECT
        pb.identificador_unico,
        pb.programa_fomento,
        pb.categoria_primeiro_acesso,
        pb.status_dado,
        CASE
            WHEN tc.id_normalizado IS NOT NULL THEN 'sim'
            ELSE 'não'
        END AS contemplado
    FROM perfil_base pb
    LEFT JOIN todos_contemplados tc
        ON REGEXP_REPLACE(pb.identificador_unico, '[^0-9]', '', 'g') = tc.id_normalizado
        AND pb.programa_fomento = tc.programa_fomento
)

SELECT
    programa_fomento,
    categoria_primeiro_acesso,
    contemplado,
    COUNT(DISTINCT identificador_unico)                                                          AS total_proponentes,
    COUNT(DISTINCT CASE WHEN status_dado = 'Confirmado' THEN identificador_unico END)           AS total_campo_preenchido,
    COUNT(DISTINCT CASE WHEN status_dado = 'Inferido'   THEN identificador_unico END)           AS total_inferido,
    ROUND(
        COUNT(DISTINCT identificador_unico)::NUMERIC
        / SUM(COUNT(DISTINCT identificador_unico)) OVER (PARTITION BY programa_fomento, contemplado)
        * 100, 2
    ) AS percentual
FROM perfil_com_contemplado
GROUP BY programa_fomento, categoria_primeiro_acesso, contemplado
ORDER BY programa_fomento, contemplado DESC, total_proponentes DESC
