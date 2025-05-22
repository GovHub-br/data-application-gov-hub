

WITH dados_pa AS (
    SELECT
        agenciabeneficiario,
        bancobeneficiario,
        codorgao,
        contabeneficiario,
        cpfbeneficiario,
        matricula,
        nomebeneficiario,
        valorultimapensao,
        cpf, 
        codvinculoservidor,
        nomealimentado,
        nomevinculoservidor
    FROM {{ source('siape', 'dados_pa') }} 
)

SELECT
    REGEXP_REPLACE(NULLIF(TRIM(agenciabeneficiario), ''), '[^0-9]', '', 'g') AS agencia_beneficiario, 
    NULLIF(TRIM(bancobeneficiario), '') AS banco_beneficiario,
    NULLIF(TRIM(codorgao), '') AS cod_orgao,
    UPPER(REGEXP_REPLACE(NULLIF(TRIM(contabeneficiario), ''), '[^0-9A-Za-z]', '', 'g')) AS conta_beneficiario, 
    REGEXP_REPLACE(NULLIF(TRIM(cpfbeneficiario), ''), '[^0-9]', '', 'g') AS cpf_beneficiario, 
    NULLIF(TRIM(matricula), '') AS matricula_servidor,
    NULLIF(TRIM(nomebeneficiario), '') AS nome_beneficiario,
    NULLIF(TRIM(valorultimapensao), '') AS valor_ultima_pensao, 
    REGEXP_REPLACE(NULLIF(TRIM(cpf), ''), '[^0-9]', '', 'g') AS cpf_servidor, 
    NULLIF(TRIM(codvinculoservidor), '') AS cod_vinculo_servidor,
    NULLIF(TRIM(nomealimentado), '') AS nome_alimentado,
    NULLIF(TRIM(nomevinculoservidor), '') AS nome_vinculo_servidor
FROM
    dados_pa