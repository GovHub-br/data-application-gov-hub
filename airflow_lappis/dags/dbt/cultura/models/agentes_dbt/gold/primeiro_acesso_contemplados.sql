{{ config(materialized='table') }}

-- Normaliza identificadores para dígitos puro, eliminando diferenças de
-- formatação entre tabelas (CPF com pontos/traço vs. só dígitos, CNPJ com
-- pontos/barra vs. só dígitos).
-- Usada em ambos os lados do JOIN para garantir correspondência consistente.
--
-- ATENÇÃO — CPF mascarado dos proponentes LPG: a base de proponentes
-- (lpg_agentes_pf/coletivos) traz o CPF anonimizado no formato
-- "***.NNN.NNN-**" (apenas os 6 dígitos centrais visíveis), enquanto
-- lpg_contemplados traz o CPF completo. Um match exato (dígito a dígito)
-- nunca ocorre para esses casos, gerando falso negativo sistemático de
-- 'contemplado' para LPG. Por isso, quando o identificador do proponente
-- vier mascarado, o JOIN usa um match parcial pelo "miolo" do CPF
-- (posições 4-9, os mesmos 6 dígitos centrais expostos pela máscara).
-- Esse match parcial tem risco de colisão entre CPFs com miolo igual.
--
-- ATENÇÃO — colunas "fantasma" em lpg_contemplados: a ingestão dinâmica de
-- planilhas (extracao_planilhas.py) não normaliza espaços/caracteres
-- invisíveis (ex.: NBSP) nos nomes de coluna, então o header "CPF ou CNPJ"
-- de arquivos diferentes pode virar colunas distintas no Postgres
-- (ex.: "cpf ou cnpj" vs. "cpf ou cnpj<NBSP>"), cada uma com parte dos
-- dados. Por isso a coluna de CPF/CNPJ é resolvida dinamicamente via
-- information_schema (qualquer coluna cujo nome contenha "cpf" e "cnpj"),
-- em vez de um nome fixo — o que tornaria a maior parte dos contemplados
-- invisível para o JOIN.

{% set cpf_cnpj_cols_query %}
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{{ source('transferegov_fundo_a_fundo', 'lpg_contemplados').schema }}'
      AND table_name = '{{ source('transferegov_fundo_a_fundo', 'lpg_contemplados').identifier }}'
      AND column_name ILIKE '%cpf%cnpj%'
    ORDER BY column_name
{% endset %}
{% set cpf_cnpj_results = run_query(cpf_cnpj_cols_query) %}
{% set cpf_cnpj_cols = cpf_cnpj_results.columns[0].values() if execute else ['cpf ou cnpj'] %}

WITH contemplados_lpg_raw AS (
    SELECT
        COALESCE(
            {% for col in cpf_cnpj_cols %}
            NULLIF(LOWER(TRIM("{{ col }}")), 'nan')
            {%- if not loop.last %},
            {% endif %}
            {% endfor %}
        ) AS cpf_cnpj_bruto
    FROM {{ source('transferegov_fundo_a_fundo', 'lpg_contemplados') }}
),

contemplados_lpg AS (
    SELECT DISTINCT
        REGEXP_REPLACE(cpf_cnpj_bruto, '[^0-9]', '', 'g') AS id_normalizado,
        'LPG' AS programa_fomento
    FROM contemplados_lpg_raw
    WHERE cpf_cnpj_bruto IS NOT NULL
      AND cpf_cnpj_bruto NOT IN ('', 'cpf ou cnpj')
      AND LENGTH(REGEXP_REPLACE(cpf_cnpj_bruto, '[^0-9]', '', 'g')) >= 11
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

-- Miolo (6 dígitos centrais) do CPF, usado para casar com identificadores
-- mascarados de proponentes ("***.NNN.NNN-**"). Só faz sentido para CPF
-- (11 dígitos); CNPJ (14 dígitos) não é mascarado na base de proponentes.
todos_contemplados_miolo AS (
    SELECT
        id_normalizado,
        programa_fomento,
        SUBSTRING(id_normalizado FROM 4 FOR 6) AS miolo_cpf
    FROM todos_contemplados
    WHERE LENGTH(id_normalizado) = 11
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
            WHEN tc.id_normalizado IS NOT NULL OR tcm.id_normalizado IS NOT NULL THEN 'sim'
            ELSE 'não'
        END AS contemplado
    FROM perfil_base pb
    LEFT JOIN todos_contemplados tc
        ON REGEXP_REPLACE(pb.identificador_unico, '[^0-9]', '', 'g') = tc.id_normalizado
        AND pb.programa_fomento = tc.programa_fomento
    LEFT JOIN todos_contemplados_miolo tcm
        ON pb.identificador_unico LIKE '%*%'
        AND LENGTH(REGEXP_REPLACE(pb.identificador_unico, '[^0-9]', '', 'g')) = 6
        AND REGEXP_REPLACE(pb.identificador_unico, '[^0-9]', '', 'g') = tcm.miolo_cpf
        AND pb.programa_fomento = tcm.programa_fomento
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
