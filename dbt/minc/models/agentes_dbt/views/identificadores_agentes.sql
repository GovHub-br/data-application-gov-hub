{{ config(materialized='view') }}

SELECT
    identificador_unico,
    historico_acesso_bruto,
    programa_fomento,
    'Pessoa Física' AS tipo_proponente
FROM {{ ref('lpg_agentes_pf') }}

UNION ALL

SELECT
    identificador_unico,
    historico_acesso_bruto,
    programa_fomento,
    'Pessoa Jurídica' AS tipo_proponente
FROM {{ ref('lpg_agentes_pj') }}

UNION ALL

SELECT
    identificador_unico,
    historico_acesso_bruto,
    programa_fomento,
    'Coletivo' AS tipo_proponente
FROM {{ ref('lpg_agentes_coletivos') }}

UNION ALL

SELECT
    identificador_unico,
    historico_acesso_bruto,
    programa_fomento,
    'Pessoa Física' AS tipo_proponente
FROM {{ ref('pnab_agentes_pf') }}

UNION ALL

SELECT
    identificador_unico,
    historico_acesso_bruto,
    programa_fomento,
    'Organização' AS tipo_proponente
FROM {{ ref('pnab_agentes_pj') }}
