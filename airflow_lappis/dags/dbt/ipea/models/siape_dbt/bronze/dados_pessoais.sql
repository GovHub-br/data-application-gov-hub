with
    dados_pessoais as (
        select
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
        from {{ source("siape", "dados_pessoais") }}
    )

select
    nullif(trim(codcor), '') as cod_cor,
    nullif(trim(codestadocivil), '') as cod_estado_civil,
    nullif(trim(codnacionalidade), '') as cod_nacionalidade,
    nullif(trim(codsexo), '') as cod_sexo,
    to_date(nullif(trim(datanascimento), ''), 'DDMMYYYY') as dt_nascimento,
    nullif(trim(gruposanguineo), '') as grupo_sanguineo,
    nullif(trim(nome), '') as nome_pessoa,
    nullif(trim(nomecor), '') as nome_cor,
    nullif(trim(nomeestadocivil), '') as nome_estado_civil,
    nullif(trim(nomemae), '') as nome_mae,
    nullif(trim(nomemunicipnasc), '') as nome_municipio_nascimento,
    nullif(trim(nomenacionalidade), '') as nome_nacionalidade,
    nullif(nullif(trim(nomepai), ''), 'NAO DECLARADO') as nome_pai,
    nullif(trim(nomesexo), '') as nome_sexo,
    regexp_replace(nullif(trim(numpispasep), ''), '[^0-9]', '', 'g') as num_pispasep,
    upper(nullif(trim(ufnascimento), '')) as uf_nascimento,
    regexp_replace(nullif(trim(cpf), ''), '[^0-9]', '', 'g') as cpf,
    nullif(trim(coddeffisica), '') as cod_deficiencia_fisica,
    nullif(trim(nomedeffisica), '') as nome_deficiencia_fisica,
    to_date(nullif(trim(datachegbrasil), ''), 'DDMMYYYY') as dt_chegada_brasil,
    nullif(trim(nomepais), '') as nome_pais_origem
from dados_pessoais
