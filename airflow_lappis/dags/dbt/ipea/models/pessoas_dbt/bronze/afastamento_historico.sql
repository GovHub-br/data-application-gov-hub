with
    afastamento_historico_raw as (
        select
            adiantamentosalarioferias,
            anoexercicio,
            datafim,
            datafimaquisicao,
            dataini,
            datainicioaquisicao,
            datainicioferiasinterrompidas,
            diasrestantes,
            gratificacaonatalina,
            numerodaparcela,
            parcelacontinuacaointerrupcao,
            parcelainterrompida,
            qtdedias,
            cpf,
            coddiplomaafastamento,
            codocorrencia,
            datapublicacaoafastamento,
            descdiplomaafastamento,
            descocorrencia,
            numerodiplomaafastamento
        -- grmatricula não está presente 
        from {{ source("siape", "afastamento_historico") }}
    ),

    afastamento_historico_cleaned as (
        -- tive que adicionar um nullif adicional pois nas colunas tinha escrito "NaN"
        -- como string, diferentemente das outras tabelas
        select
            nullif(
                nullif(nullif(trim(adiantamentosalarioferias), ''), 'NaN'), '[null]'
            ) as adiantamentosalarioferias_clean,
            nullif(
                nullif(nullif(trim(anoexercicio), ''), 'NaN'), '[null]'
            ) as anoexercicio_clean,
            nullif(nullif(nullif(trim(datafim), ''), 'NaN'), '[null]') as datafim_clean,
            nullif(
                nullif(nullif(trim(datafimaquisicao), ''), 'NaN'), '[null]'
            ) as datafimaquisicao_clean,
            nullif(nullif(nullif(trim(dataini), ''), 'NaN'), '[null]') as dataini_clean,
            nullif(
                nullif(nullif(trim(datainicioaquisicao), ''), 'NaN'), '[null]'
            ) as datainicioaquisicao_clean,
            nullif(
                nullif(nullif(trim(datainicioferiasinterrompidas), ''), 'NaN'), '[null]'
            ) as datainicioferiasinterrompidas_clean,
            nullif(
                nullif(nullif(trim(diasrestantes), ''), 'NaN'), '[null]'
            ) as diasrestantes_clean,
            nullif(
                nullif(nullif(trim(gratificacaonatalina), ''), 'NaN'), '[null]'
            ) as gratificacaonatalina_clean,
            nullif(
                nullif(nullif(trim(numerodaparcela), ''), 'NaN'), '[null]'
            ) as numerodaparcela_clean,
            nullif(
                nullif(nullif(trim(parcelacontinuacaointerrupcao), ''), 'NaN'), '[null]'
            ) as parcelacontinuacaointerrupcao_clean,
            nullif(
                nullif(nullif(trim(parcelainterrompida), ''), 'NaN'), '[null]'
            ) as parcelainterrompida_clean,
            nullif(nullif(nullif(trim(qtdedias), ''), 'NaN'), '[null]') as qtdedias_clean,
            nullif(nullif(nullif(trim(cpf), ''), 'NaN'), '[null]') as cpf_clean,
            nullif(
                nullif(nullif(trim(coddiplomaafastamento), ''), 'NaN'), '[null]'
            ) as coddiplomaafastamento_clean,
            nullif(
                nullif(nullif(trim(codocorrencia), ''), 'NaN'), '[null]'
            ) as codocorrencia_clean,
            nullif(
                nullif(nullif(trim(datapublicacaoafastamento), ''), 'NaN'), '[null]'
            ) as datapublicacaoafastamento_clean,
            nullif(
                nullif(nullif(trim(descdiplomaafastamento), ''), 'NaN'), '[null]'
            ) as descdiplomaafastamento_clean,
            nullif(
                nullif(nullif(trim(descocorrencia), ''), 'NaN'), '[null]'
            ) as descocorrencia_clean,
            nullif(
                nullif(nullif(trim(numerodiplomaafastamento), ''), 'NaN'), '[null]'
            ) as numerodiplomaafastamento_clean
        from afastamento_historico_raw
    )

select
    null as gr_matricula,  -- placeholder para matrícula, pois aqui na tabela histórica não tem ...
    adiantamentosalarioferias_clean as adiantamento_salario_ferias,
    anoexercicio_clean as ano_exercicio,
    -- mesma logica dos dados_afastamento, garantindo os comprimentos corretos
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
    case
        when length(datainicioferiasinterrompidas_clean) = 8
        then to_date(datainicioferiasinterrompidas_clean, 'DDMMYYYY')
        else null
    end as dt_inicio_ferias_interrompidas,
    cast(diasrestantes_clean as int) as dias_restantes,
    gratificacaonatalina_clean as gratificacao_natalina,
    cast(numerodaparcela_clean as int) as numero_parcela,
    parcelacontinuacaointerrupcao_clean as parcela_continuacao_interrupcao,
    parcelainterrompida_clean as parcela_interrompida,
    cast(qtdedias_clean as int) as qtde_dias,
    regexp_replace(cpf_clean, '[^0-9]', '', 'g') as cpf,
    coddiplomaafastamento_clean as cod_diploma_afastamento,
    codocorrencia_clean as cod_ocorrencia,
    case
        when length(datapublicacaoafastamento_clean) = 8
        then to_date(datapublicacaoafastamento_clean, 'YYYYMMDD')  -- Formato YYYYMMDD
        else null
    end as dt_publicacao_afastamento,
    descdiplomaafastamento_clean as desc_diploma_afastamento,
    descocorrencia_clean as desc_ocorrencia,
    cast(numerodiplomaafastamento_clean as int) as numero_diploma_afastamento
from afastamento_historico_cleaned
