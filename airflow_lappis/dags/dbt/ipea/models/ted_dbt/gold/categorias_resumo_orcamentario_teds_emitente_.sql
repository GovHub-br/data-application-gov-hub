-- Transformando o resumo orçamentário em categorias para utilizar no gráfico de barras empilhadas
select
    plano_acao,
    num_transf,
    '1.1 Valor firmado' as tipo,
    valor_firmado as valor,
    '1. Valor firmado' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

---------------------------------------------------------------------------------------------
select
    plano_acao,
    num_transf,
    '2.1 Destaque total enviado' as tipo,
    (orcamento_recebido - orcamento_devolvido) as valor,
    '2. Destaque orçamentário' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '2.2 Destaque a enviar' as tipo,
    (valor_firmado - (orcamento_recebido - orcamento_devolvido)) as valor,
    '2. Destaque orçamentário' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

---------------------------------------------------------------------------------------------

union all

select
    plano_acao,
    num_transf,
    '3.1 Financeiro enviado' as tipo,
    (financeiro_recebido - (financeiro_devolvido + financeiro_cancelado)) as valor,
    '3. Repasse financeiro' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '3.2 Financeiro a enviar (em relação ao orçamento)' as tipo,
    (orcamento_recebido - orcamento_devolvido - (financeiro_recebido - (financeiro_devolvido + financeiro_cancelado))) as valor,
    '3. Repasse financeiro' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}
