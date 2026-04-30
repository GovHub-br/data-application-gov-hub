with

    categorias_emitente as (
        select
            plano_acao,
            num_transf,
            case
                when tipo = '2.1 Destaque total enviado'
                then '2.1 Destaque total recebido'
                when tipo = '2.2 Destaque a enviar'
                then '2.2 Destaque a receber'
                when tipo = '3.1 Financeiro enviado'
                then '4.1 Financeiro recebido'
                when tipo = '3.2 Financeiro a enviar (em relação ao orçamento)'
                then '4.2 Financeiro a receber (em relação ao orçamento)'
                else tipo
            end as tipo,
            valor,
            case
                when categoria = '3. Repasse financeiro'
                then '4. Repasse financeiro'
                else categoria
            end as categoria,
            dt_ingest
        from {{ ref("categorias_resumo_orcamentario_teds_emitente_") }}
    ),

    execucao_orcamentaria as (
        select
            plano_acao,
            num_transf,
            '3.1 Total empenhado' as tipo,
            (empenhado - empenho_anulado) as valor,
            '3. Execução orçamentária' as categoria,
            dt_ingest
        from {{ ref("ted_resumo_orcamentario") }}

        union all

        select
            plano_acao,
            num_transf,
            '3.2 A empenhar' as tipo,
            (
                (orcamento_recebido - orcamento_devolvido)
                - (empenhado - empenho_anulado)
            ) as valor,
            '3. Execução orçamentária' as categoria,
            dt_ingest
        from {{ ref("ted_resumo_orcamentario") }}
    )

select *
from categorias_emitente

union all

select *
from execucao_orcamentaria
