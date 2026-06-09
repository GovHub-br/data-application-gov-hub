{{ config(
    materialized='incremental',
    unique_key='identificador_unico'
) }}

SELECT
    LOWER(TRIM("nº do cpf"))                                  AS identificador_unico,
    LOWER(TRIM("já acessou recursos públicos do fomento à cultura anteriorme")) AS ja_acessou_recursos_bruto
FROM {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_fisica') }}

{% if is_incremental() %}
    WHERE LOWER(TRIM("nº do cpf")) NOT IN (
        SELECT identificador_unico FROM {{ this }}
    )
{% endif %}
