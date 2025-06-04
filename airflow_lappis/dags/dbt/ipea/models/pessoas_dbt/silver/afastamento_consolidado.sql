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
    'dados_afastamento' as origem_dados  -- identificar a fonte
from {{ ref("dados_afastamento") }}

union all

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
    null as gr_matricula,  -- não tem na afastamneto historico ...
    'afastamento_historico' as origem_dados  -- identificar a fonte
from {{ ref("afastamento_historico") }}
