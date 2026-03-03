-- Transformando o resumo orçamentário em categorias para utilizar no gráfico de barras empilhadas
select
    plano_acao,
    num_transf,
    '1.1 Destaque total recebido' as tipo,
    (orcamento_recebido - orcamento_devolvido) as valor,
    '1. Destaque orçamentário' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '1.2 Destaque a receber' as tipo,
    (valor_firmado - orcamento_recebido + orcamento_devolvido) as valor,
    '1. Destaque orçamentário' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

---------------------------------------------------------------------------------------------

union all

select
    plano_acao,
    num_transf,
    '2.1 Empenhado (total-anulado)' as tipo,
    (empenhado - empenho_anulado) as valor,
    '2. Visão geral da exec. orçamentária' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '2.2 A empenhar' as tipo,
    (orcamento_recebido - orcamento_devolvido) - (empenhado - empenho_anulado) as valor,
    '2. Visão geral da exec. orçamentária' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

---------------------------------------------------------------------------------------------

union all

select
    plano_acao,
    num_transf,
    '3.1 Despesas pagas no exercício' as tipo,
    despesas_pagas_exercicio as valor,
    '3. Detalhe da exec. orçamentária' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '3.2 Despesas pagas (RAP)' as tipo,
    despesas_pagas_rap as valor,
    '3. Detalhe da exec. orçamentária' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '3.3 Saldo empenho' as tipo,
    (empenhado - empenho_anulado) - (despesas_pagas_exercicio + despesas_pagas_rap) as valor,
    '3. Detalhe da exec. orçamentária' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}
