select
    ano_exercicio,
    unidade_orcamentaria,
    unidade_orcamentaria_desc,
    acao_governo,
    acao_governo_desc,
    programa_governo,
    programa_governo_desc,
    unidade_plano_orcamentario,
    plano_orcamentario_1,
    plano_orcamentario_2,
    programa_plano_orcamentario,
    acao_plano_orcamentario,
    plano_orcamentario_6,
    plano_orcamentario_desc,
    elemento_despesa,
    elemento_despesa_desc,
    orgao_uge,
    orgao_uge_desc,
    uge_matriz_filial,
    ug_executora,
    ug_executora_desc,
    projeto_inicial_loa,
    dotacao_inicial,
    dotacao_atualizada,
    credito_disponivel,
    despesas_empenhadas,
    despesas_a_liquidar,
    despesar_a_pagar,
    despesas_pagas,
    restos_a_pagar_inscritos,
    restos_a_pagar_pagos,
    -- Métricas derivadas para o dashboard
    (dotacao_atualizada - despesas_empenhadas) as saldo_orcamentario,
    case
        when dotacao_atualizada > 0
        then round((despesas_empenhadas / dotacao_atualizada) * 100, 2)
        else 0
    end as percentual_execucao,
    dt_ingest
from {{ ref('visao_orcamentaria_total') }}
