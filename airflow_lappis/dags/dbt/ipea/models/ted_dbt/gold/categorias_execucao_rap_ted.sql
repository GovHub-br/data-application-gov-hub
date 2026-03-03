-- Transformando o resumo orçamentário em categorias para utilizar no gráfico de barras empilhadas
select
    plano_acao,
    num_transf,
    '1.1 Restos a pagar inscritos' as tipo,
    restos_a_pagar as valor,
    '1. Restos a pagar inscritos' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '2.1 Despesas pagas RAP' as tipo,
    despesas_pagas_rap as valor,
    '2. Detalhe da exec. orçamentária - Restos a Pagar' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}

union all

select
    plano_acao,
    num_transf,
    '2.2 Saldo RAP' as tipo,
    restos_a_pagar - despesas_pagas_rap as valor,
    '2. Detalhe da exec. orçamentária - Restos a Pagar' as categoria,
    dt_ingest
from {{ ref('ted_resumo_orcamentario') }}
