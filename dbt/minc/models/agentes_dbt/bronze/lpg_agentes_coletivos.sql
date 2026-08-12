{{ config(
    materialized='incremental',
    unique_key='identificador_unico'
) }}

SELECT
    LOWER(TRIM("nº do cpf do representante do grupo/coletivo")) AS identificador_unico,
    LOWER(TRIM("já acessou recursos públicos do fomento à cultura anteriorme")) AS historico_acesso_bruto,
    'LPG' AS programa_fomento
FROM {{ source('relatorio_gestao', 'planilha_dados_lpg') }}
WHERE tabela_origem = 'lpg_dados_coletivos'

{% if is_incremental() %}
  AND LOWER(TRIM("nº do cpf do representante do grupo/coletivo")) NOT IN (
    SELECT identificador_unico FROM {{ this }}
)
{% endif %}
