with
    dados_uorg as (
        select
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
        from {{ source("siape", "dados_uorg") }}
    )

select
    nullif(trim(bairrouorg), '') as bairro_uorg,
    regexp_replace(nullif(trim(cepuorg), ''), '[^0-9]', '', 'g') as cep_uorg,
    nullif(trim(codmatricula), '') as codigo_matricula,
    nullif(trim(codmunicipiouorg), '') as codigo_municipio_uorg,
    nullif(trim(codorgao), '') as codigo_orgao,
    nullif(trim(codorgaouorg), '') as codigo_orgao_u
    lower(nullif(trim(emailuorg), '')) as email_uorg,
    nullif(trim(enduorg), '') as tipo_endereco_uorg,
    nullif(trim(logradourouorg), '') as logradouro_uorg,
    nullif(trim(nomemunicipiouorg), '') as nome_municipio_uorg,
    nullif(trim(nomeuorg), '') as nome_uorg,
    regexp_replace(nullif(trim(numtelefoneuorg), ''), '[^0-9]', '', 'g') as telefone_uorg,
    nullif(trim(numerouorg), '') as numero_endereco_uorg,
    nullif(trim(siglauorg), '') as sigla_uorg,
    upper(nullif(trim(ufuorg), '')) as uf_uorg,
    regexp_replace(nullif(trim(cpf), ''), '[^0-9]', '', 'g') as cpf,
    nullif(nullif(trim(complementouorg), ''), '---') as complemento_endereco_uorg,
    regexp_replace(nullif(trim(numfaxuorg), ''), '[^0-9]', '', 'g') as fax_uorg
from dados_uorg
