

WITH dados_pessoais AS (
    SELECT
        codcor,
        codestadocivil,
        codnacionalidade,
        codsexo,
        datanascimento,
        gruposanguineo,
        nome,
        nomecor,
        nomeestadocivil,
        nomemae,
        nomemunicipnasc,
        nomenacionalidade,
        nomepai,
        nomesexo,
        numpispasep,
        ufnascimento,
        cpf,
        coddeffisica,
        nomedeffisica,
        datachegbrasil,
        nomepais
    FROM {{ source('siape', 'dados_pessoais') }} 
)

SELECT
    NULLIF(TRIM(codcor), '') AS cod_cor,
    NULLIF(TRIM(codestadocivil), '') AS cod_estado_civil,
    NULLIF(TRIM(codnacionalidade), '') AS cod_nacionalidade,
    NULLIF(TRIM(codsexo), '') AS cod_sexo,
    TO_DATE(NULLIF(TRIM(datanascimento), ''), 'DDMMYYYY') AS dt_nascimento,
    NULLIF(TRIM(gruposanguineo), '') AS grupo_sanguineo,
    NULLIF(TRIM(nome), '') AS nome_pessoa,
    NULLIF(TRIM(nomecor), '') AS nome_cor,
    NULLIF(TRIM(nomeestadocivil), '') AS nome_estado_civil,
    NULLIF(TRIM(nomemae), '') AS nome_mae,
    NULLIF(TRIM(nomemunicipnasc), '') AS nome_municipio_nascimento,
    NULLIF(TRIM(nomenacionalidade), '') AS nome_nacionalidade,
    NULLIF(NULLIF(TRIM(nomepai), ''), 'NAO DECLARADO') AS nome_pai, 
    NULLIF(TRIM(nomesexo), '') AS nome_sexo,
    REGEXP_REPLACE(NULLIF(TRIM(numpispasep), ''), '[^0-9]', '', 'g') AS num_pispasep,
    UPPER(NULLIF(TRIM(ufnascimento), '')) AS uf_nascimento,
    REGEXP_REPLACE(NULLIF(TRIM(cpf), ''), '[^0-9]', '', 'g') AS cpf,
    NULLIF(TRIM(coddeffisica), '') AS cod_deficiencia_fisica,
    NULLIF(TRIM(nomedeffisica), '') AS nome_deficiencia_fisica,
    TO_DATE(NULLIF(TRIM(datachegbrasil), ''), 'DDMMYYYY') AS dt_chegada_brasil,
    NULLIF(TRIM(nomepais), '') AS nome_pais_origem
FROM
    dados_pessoais