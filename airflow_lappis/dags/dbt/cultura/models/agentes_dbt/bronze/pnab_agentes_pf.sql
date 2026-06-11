{{ config(
    materialized='incremental',
    unique_key='identificador_unico'
) }}

SELECT
    LOWER(TRIM("nº do cpf")) AS identificador_unico,
    LOWER(TRIM("já acessou recursos públicos do fomento à cultura nos últim")) AS historico_acesso_bruto,
    'PNAB' AS programa_fomento
FROM {{ source('transferegov_fundo_a_fundo', 'pnab_pessoas') }}

{% if is_incremental() %}
WHERE LOWER(TRIM("nº do cpf")) NOT IN (
    SELECT identificador_unico FROM {{ this }}
)
{% endif %}
