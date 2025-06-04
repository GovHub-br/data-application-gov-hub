with
    dados_dependentes_raw as (
        select
            codcondicao,
            codgrauparentesco,
            codorgao,
            cpf,
            matricula,
            nome,
            nomecondicao,
            nomegrauparentesco,
            codbeneficio,
            datafim,
            datainicio,
            nomebeneficio
        from {{ source("siape", "dados_dependentes") }}
    ),

    dados_dependentes_cleaned as (
        select
            nullif(trim(codcondicao), 'NaN') as codcondicao,
            nullif(trim(codgrauparentesco), 'NaN') as codgrauparentesco,
            nullif(trim(codorgao), 'NaN') as codorgao,
            nullif(trim(cpf), 'NaN') as cpf,
            nullif(trim(matricula), 'NaN') as matricula,
            nullif(trim(nome), 'NaN') as nome,
            nullif(trim(nomecondicao), 'NaN') as nomecondicao,
            nullif(trim(nomegrauparentesco), 'NaN') as nomegrauparentesco,
            nullif(trim(codbeneficio), 'NaN') as codbeneficio,
            nullif(trim(datafim), 'NaN') as datafim,
            nullif(trim(datainicio), 'NaN') as datainicio,
            nullif(trim(nomebeneficio), 'NaN') as nomebeneficio
        from dados_dependentes_raw
    )

select
    nullif(codcondicao, '') as cod_condicao,
    nullif(codgrauparentesco, '') as cod_grau_parentesco,
    nullif(codorgao, '') as cod_orgao,
    regexp_replace(nullif(cpf, ''), '[^0-9]', '', 'g') as cpf,
    nullif(matricula, '') as matricula,
    nullif(nome, '') as nome_dependente,
    nullif(nomecondicao, '') as nome_condicao,
    nullif(nomegrauparentesco, '') as nome_grau_parentesco,
    nullif(codbeneficio, '') as cod_beneficio,
    -- Converte para DATE, tratando '', 'NaN' e '00000000' como NULL
    to_date(nullif(nullif(datafim, ''), '00000000'), 'DDMMYYYY') as dt_fim,
    to_date(nullif(nullif(datainicio, ''), '00000000'), 'DDMMYYYY') as dt_inicio,
    nullif(nomebeneficio, '') as nome_beneficio
from dados_dependentes_cleaned
