-- Transformando o resumo orçamentário em categorias para utilizar no gráfico de barras empilhadas
select
    plano_acao,
    num_transf,
    '1.1 Orçamento recebido' as tipo,
    (orcamento_recebido - orcamento_devolvido) as valor,
    '1. Orçamento recebido' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all
---------------------------------------------------------------------------------------------
select
    plano_acao,
    num_transf,
    '2.1 Financeiro recebido' as tipo,
    (financeiro_recebido - (financeiro_devolvido + financeiro_cancelado)) as valor,
    '2. Visão geral repasses financeiros' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '2.2 Financeiro a receber (em relação ao orçamento)' as tipo,
    (orcamento_recebido - orcamento_devolvido - (financeiro_recebido - (financeiro_devolvido + financeiro_cancelado))) as valor,
    '2. Visão geral repasses financeiros' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '3.1 Despesas pagas no exercício' as tipo,
    despesas_pagas_exercicio as valor,
    '3. Detalhe da execução financeira' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '3.2 Despesas pagas RAP' as tipo,
    despesas_pagas_rap as valor,
    '3. Detalhe da execução financeira' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '3.3 Saldo financeiro' as tipo,
    (financeiro_recebido - (financeiro_devolvido + financeiro_cancelado + despesas_pagas_exercicio + despesas_pagas_rap)) as valor,
    '3. Detalhe da execução financeira' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}
