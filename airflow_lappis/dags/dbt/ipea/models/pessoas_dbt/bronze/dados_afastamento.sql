with
    dados_afastamento_raw as (
        select
            adiantamentosalarioferias,
            anoexercicio,
            datafim,
            datafimaquisicao,
            dataini,
            datainicioaquisicao,
            gratificacaonatalina,
            numerodaparcela,
            parcelacontinuacaointerrupcao,
            parcelainterrompida,
            qtdedias,
            grmatricula,
            cpf,
            coddiplomaafastamento,
            codocorrencia,
            datapublicacaoafastamento,
            descdiplomaafastamento,
            descocorrencia,
            numerodiplomaafastamento,
            datainicioferiasinterrompidas,
            diasrestantes
        from {{ source("siape", "dados_afastamento") }}
    ),

    dados_afastamento_cleaned as (
        select
            nullif(
                trim(adiantamentosalarioferias), ''
            ) as adiantamentosalarioferias_clean,
            nullif(trim(anoexercicio), '') as anoexercicio_clean,
            nullif(trim(datafim), '') as datafim_clean,
            nullif(trim(datafimaquisicao), '') as datafimaquisicao_clean,
            nullif(trim(dataini), '') as dataini_clean,
            nullif(trim(datainicioaquisicao), '') as datainicioaquisicao_clean,
            nullif(trim(gratificacaonatalina), '') as gratificacaonatalina_clean,
            nullif(trim(numerodaparcela), '') as numerodaparcela_clean,
            nullif(
                trim(parcelacontinuacaointerrupcao), ''
            ) as parcelacontinuacaointerrupcao_clean,
            nullif(trim(parcelainterrompida), '') as parcelainterrompida_clean,
            nullif(trim(qtdedias), '') as qtdedias_clean,
            nullif(trim(grmatricula), '') as grmatricula_clean,
            nullif(trim(cpf), '') as cpf_clean,
            nullif(trim(coddiplomaafastamento), '') as coddiplomaafastamento_clean,
            nullif(trim(codocorrencia), '') as codocorrencia_clean,
            nullif(
                trim(datapublicacaoafastamento), ''
            ) as datapublicacaoafastamento_clean,
            nullif(trim(descdiplomaafastamento), '') as descdiplomaafastamento_clean,
            nullif(trim(descocorrencia), '') as descocorrencia_clean,
            nullif(trim(numerodiplomaafastamento), '') as numerodiplomaafastamento_clean,
            nullif(
                trim(datainicioferiasinterrompidas), ''
            ) as datainicioferiasinterrompidas_clean,
            nullif(trim(diasrestantes), '') as diasrestantes_clean
        from dados_afastamento_raw
    )

select
    adiantamentosalarioferias_clean as adiantamento_salario_ferias,
    anoexercicio_clean as ano_exercicio,
    -- adicionei esses checks pois tinham algumas strings de datas retornando "0" e
    -- quebrando o to_date ...
    case
        when length(datafim_clean) = 8 then to_date(datafim_clean, 'DDMMYYYY') else null
    end as dt_fim,
    case
        when length(datafimaquisicao_clean) = 8
        then to_date(datafimaquisicao_clean, 'DDMMYYYY')
        else null
    end as dt_fim_aquisicao,
    case
        when length(dataini_clean) = 8 then to_date(dataini_clean, 'DDMMYYYY') else null
    end as dt_ini,
    case
        when length(datainicioaquisicao_clean) = 8
        then to_date(datainicioaquisicao_clean, 'DDMMYYYY')
        else null
    end as dt_inicio_aquisicao,
    gratificacaonatalina_clean as gratificacao_natalina,
    cast(numerodaparcela_clean as int) as numero_parcela,
    parcelacontinuacaointerrupcao_clean as parcela_continuacao_interrupcao,
    parcelainterrompida_clean as parcela_interrompida,
    cast(qtdedias_clean as int) as qtde_dias,
    grmatricula_clean as gr_matricula,
    regexp_replace(cpf_clean, '[^0-9]', '', 'g') as cpf,
    coddiplomaafastamento_clean as cod_diploma_afastamento,
    codocorrencia_clean as cod_ocorrencia,
    case
        when length(datapublicacaoafastamento_clean) = 8
        then to_date(datapublicacaoafastamento_clean, 'YYYYMMDD')  -- Essa veio diferente, n sei o pq
        else null
    end as dt_publicacao_afastamento,
    descdiplomaafastamento_clean as desc_diploma_afastamento,
    descocorrencia_clean as desc_ocorrencia,
    cast(numerodiplomaafastamento_clean as int) as numero_diploma_afastamento,
    case
        when length(datainicioferiasinterrompidas_clean) = 8
        then to_date(datainicioferiasinterrompidas_clean, 'DDMMYYYY')
        else null
    end as dt_inicio_ferias_interrompidas,
    cast(diasrestantes_clean as int) as dias_restantes
from dados_afastamento_cleaned
