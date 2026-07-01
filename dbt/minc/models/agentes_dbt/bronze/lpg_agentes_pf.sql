{{ config(
    materialized='incremental',
    unique_key='identificador_unico'
) }}

SELECT
    LOWER(TRIM("nº do cpf")) AS identificador_unico,
    LOWER(TRIM("já acessou recursos públicos do fomento à cultura anteriorme")) AS historico_acesso_bruto,
    'LPG' AS programa_fomento
FROM {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_fisica') }}
WHERE "nº do cpf" IS NOT NULL
  AND LOWER(TRIM("nº do cpf")) NOT IN ('', 'nan', 'none')

{% if is_incremental() %}
  AND LOWER(TRIM("nº do cpf")) NOT IN (
      SELECT identificador_unico FROM {{ this }}
  )
{% endif %}