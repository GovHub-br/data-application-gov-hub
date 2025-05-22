WITH dados_uorg AS (
    SELECT
        bairrouorg,
        cepuorg,
        codmatricula,
        codmunicipiouorg,
        codorgao,
        codorgaouorg,
        emailuorg,
        enduorg,
        logradourouorg,
        nomemunicipiouorg,
        nomeuorg,
        numtelefoneuorg,
        numerouorg,
        siglauorg,
        ufuorg,
        cpf,
        complementouorg,
        numfaxuorg
    FROM {{ source('siape', 'dados_uorg') }}
)

SELECT
    NULLIF(TRIM(bairrouorg), '') AS bairro_uorg,
    REGEXP_REPLACE(NULLIF(TRIM(cepuorg), ''), '[^0-9]', '', 'g') AS cep_uorg, 
    NULLIF(TRIM(codmatricula), '') AS codigo_matricula, 
    NULLIF(TRIM(codmunicipiouorg), '') AS codigo_municipio_uorg,
    NULLIF(TRIM(codorgao), '') AS codigo_orgao,
    NULLIF(TRIM(codorgaouorg), '') AS codigo_orgao_u
    LOWER(NULLIF(TRIM(emailuorg), '')) AS email_uorg,
    NULLIF(TRIM(enduorg), '') AS tipo_endereco_uorg, 
    NULLIF(TRIM(logradourouorg), '') AS logradouro_uorg,
    NULLIF(TRIM(nomemunicipiouorg), '') AS nome_municipio_uorg,
    NULLIF(TRIM(nomeuorg), '') AS nome_uorg,
    REGEXP_REPLACE(NULLIF(TRIM(numtelefoneuorg), ''), '[^0-9]', '', 'g') AS telefone_uorg, 
    NULLIF(TRIM(numerouorg), '') AS numero_endereco_uorg, 
    NULLIF(TRIM(siglauorg), '') AS sigla_uorg,
    UPPER(NULLIF(TRIM(ufuorg), '')) AS uf_uorg,
    REGEXP_REPLACE(NULLIF(TRIM(cpf), ''), '[^0-9]', '', 'g') AS cpf,
    NULLIF(NULLIF(TRIM(complementouorg), ''), '---') AS complemento_endereco_uorg,
    REGEXP_REPLACE(NULLIF(TRIM(numfaxuorg), ''), '[^0-9]', '', 'g') AS fax_uorg
FROM
    dados_uorg
