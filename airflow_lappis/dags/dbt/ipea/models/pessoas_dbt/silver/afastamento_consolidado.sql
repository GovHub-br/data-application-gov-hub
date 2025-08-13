with

    dados_afastamento_totais as (
        select distinct
            adiantamento_salario_ferias,
            ano_exercicio,
            dt_fim,
            dt_fim_aquisicao,
            dt_ini,
            dt_inicio_aquisicao,
            dt_inicio_ferias_interrompidas,
            dias_restantes,
            gratificacao_natalina,
            numero_parcela,
            parcela_continuacao_interrupcao,
            parcela_interrompida,
            qtde_dias,
            cpf,
            cod_diploma_afastamento,
            cod_ocorrencia,
            dt_publicacao_afastamento,
            desc_diploma_afastamento,
            desc_ocorrencia,
            numero_diploma_afastamento,
            gr_matricula,
            'dados_afastamento' as origem_dados  -- identificar a fonte
        from {{ ref("dados_afastamento") }}

        union all

        select distinct
            adiantamento_salario_ferias,
            ano_exercicio,
            dt_fim,
            dt_fim_aquisicao,
            dt_ini,
            dt_inicio_aquisicao,
            dt_inicio_ferias_interrompidas,
            dias_restantes,
            gratificacao_natalina,
            numero_parcela,
            parcela_continuacao_interrupcao,
            parcela_interrompida,
            qtde_dias,
            cpf,
            cod_diploma_afastamento,
            cod_ocorrencia,
            dt_publicacao_afastamento,
            desc_diploma_afastamento,
            desc_ocorrencia,
            numero_diploma_afastamento,
            null as gr_matricula,  -- não tem na afastamneto historico ...
            'afastamento_historico' as origem_dados  -- identificar a fonte
        from {{ ref("afastamento_historico") }}
    ),

    nomes_dt as (select distinct nome_pessoa, cpf from {{ ref("dados_pessoais") }}),

    funcoes_chefia as (
        select distinct cpf, cod_funcao, sigla_uorg_exercicio
        from {{ ref("dados_funcionais") }}
    ),

    -- Retirando duplicatas entre afastamento_historico e dados_afastamento
    grupamentos as (
        select *, rank() over (partition by cpf order by dt_ini) as ordenacao
        from dados_afastamento_totais
    ),

    prioridades as (
        select
            *,
            row_number() over (
                partition by cpf, ordenacao
                order by
                    case
                        when origem_dados = 'dados_afastamento'
                        then 1
                        when origem_dados = 'afastamento_historico'
                        then 2
                    end
            ) as prioridade
        from grupamentos
    ),

    resultado as (
        select
            adiantamento_salario_ferias,
            ano_exercicio,
            dt_fim,
            dt_fim_aquisicao,
            dt_ini,
            dt_inicio_aquisicao,
            dt_inicio_ferias_interrompidas,
            dias_restantes,
            gratificacao_natalina,
            numero_parcela,
            parcela_continuacao_interrupcao,
            parcela_interrompida,
            qtde_dias,
            cpf,
            cod_diploma_afastamento,
            cod_ocorrencia,
            dt_publicacao_afastamento,
            desc_diploma_afastamento,
            desc_ocorrencia,
            numero_diploma_afastamento,
            gr_matricula,
            origem_dados
        from prioridades
        where prioridade = 1
    )

select *
from resultado
left join nomes_dt using (cpf)
left join funcoes_chefia using (cpf)
