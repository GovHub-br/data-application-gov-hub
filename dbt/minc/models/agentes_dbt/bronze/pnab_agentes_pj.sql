{{ config(
    materialized='incremental',
    unique_key='identificador_unico'
) }}

SELECT
    LOWER(TRIM("nº do cnpj")) AS identificador_unico,
    LOWER(TRIM("já acessou recursos públicos do fomento à cultura anteriorment")) AS historico_acesso_bruto,
    'PNAB' AS programa_fomento
FROM {{ source('relatorio_gestao', 'planilha_dados_pnab_ciclo_1') }}
WHERE tabela_origem = 'pnab_organizacoes'

{% if is_incremental() %}
  AND LOWER(TRIM("nº do cnpj")) NOT IN (
    SELECT identificador_unico FROM {{ this }}
)
{% endif %}
