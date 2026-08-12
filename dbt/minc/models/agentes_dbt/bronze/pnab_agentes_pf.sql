{{ config(
    materialized='incremental',
    unique_key='identificador_unico'
) }}

SELECT
    LOWER(TRIM("nº do cpf")) AS identificador_unico,
    LOWER(TRIM("já acessou recursos públicos do fomento à cultura nos últim")) AS historico_acesso_bruto,
    'PNAB' AS programa_fomento
FROM {{ source('relatorio_gestao', 'planilha_dados_pnab_ciclo_1') }}
WHERE tabela_origem = 'pnab_pessoas'
  AND "nº do cpf" IS NOT NULL
  AND LOWER(TRIM("nº do cpf")) NOT IN ('', 'nan', 'none')

{% if is_incremental() %}
  AND LOWER(TRIM("nº do cpf")) NOT IN (
      SELECT identificador_unico FROM {{ this }}
  )
{% endif %}
