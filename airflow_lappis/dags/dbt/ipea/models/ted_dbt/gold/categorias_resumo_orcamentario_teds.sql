-- Transformando o resumo orçamentário em categorias para utilizar no gráfico de barras
-- empilhadas
select
    plano_acao,
    num_transf,
    '1.1 Valor firmado' as tipo,
    valor_firmado as valor,
    '1. Valor firmado' as categoria
from {{ ref("ted_resumo_orcamentario") }}
union all
-- -------------------------------------------------------------------------------------------
select
    plano_acao,
    num_transf,
    '2.1 Destaque total recebido' as tipo,
    (orcamento_recebido - orcamento_devolvido) as valor,
    '2. Destaque orçamentário' as categoria
from {{ ref("ted_resumo_orcamentario") }}
union all
select
    plano_acao,
    num_transf,
    '2.2 Destaque a receber' as tipo,
    (valor_firmado - (orcamento_recebido - orcamento_devolvido)) as valor,
    '2. Destaque orçamentário' as categoria
from {{ ref("ted_resumo_orcamentario") }}
-- -------------------------------------------------------------------------------------------
union all
select
    plano_acao,
    num_transf,
    '3.1 Total empenhado' as tipo,
    (empenhado - empenho_anulado) as valor,
    '3. Execução orçamentária' as categoria
from {{ ref("ted_resumo_orcamentario") }}
union all
select
    plano_acao,
    num_transf,
    '3.2 A empenhar' as tipo,
    ((orcamento_recebido - orcamento_devolvido) - (empenhado - empenho_anulado)) as valor,
    '3. Execução orçamentária' as categoria
from {{ ref("ted_resumo_orcamentario") }}
-- -------------------------------------------------------------------------------------------
union all
select
    plano_acao,
    num_transf,
    '4.1 Financeiro recebido' as tipo,
    (financeiro_recebido - (financeiro_devolvido + financeiro_cancelado)) as valor,
    '4. Repasse financeiro' as categoria
from {{ ref("ted_resumo_orcamentario") }}
union all
select
    plano_acao,
    num_transf,
    '4.2 Financeiro a receber (em relação ao orçamento)' as tipo,
    (
        orcamento_recebido
        - orcamento_devolvido
        - (financeiro_recebido - (financeiro_devolvido + financeiro_cancelado))
    ) as valor,
    '4. Repasse financeiro' as categoria
from {{ ref("ted_resumo_orcamentario") }}
