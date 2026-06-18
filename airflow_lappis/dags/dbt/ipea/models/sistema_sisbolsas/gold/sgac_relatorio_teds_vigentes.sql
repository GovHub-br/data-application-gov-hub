with
    projetos as (
        select *
        from {{ ref("sgac_projetos_sgac") }}
        where data_vencimento >= current_date
          and coalesce(status, '') ilike '%execu%'
          and (
            instrumento ilike '%TED%'
            or instrumento ilike '%execução descentralizada%'
            or instrumento ilike '%Dispensa de TED%'
          )
    ),

    teds_siafi as (
        select
            sgac_id,
            sum(coalesce(despesas_pagas_exercicio, 0))
                as despesas_pagas_exercicio,
            sum(coalesce(despesas_pagas_rap, 0)) as despesas_pagas_rap,
            max(valor_firmado) as valor_firmado
        from {{ ref("sgac_teds_siafi") }}
        group by sgac_id
    ),

    relatorio as (
        select
            p.entidades_externas,
            p.instrumento,
            p.diretoria_responsavel as diretoria,
            p.data_inicio as inicio_execucao,
            p.data_vencimento as data_de_conclusao,
            p.numero_do_proc as numero_do_processo,
            p.recursos_orcamentarios as recursos_externos,
            p.recursos_nao_orcamentarios as recursos_ipea,
            p.numero_siafi,
            p.termos_aditivos,
            null::text as apostilamentos,
            null::text as prorrogacao_oficio,
            null::text as planejamento_diarias_passagens,
            case
                when s.sgac_id is not null
                then coalesce(s.despesas_pagas_exercicio, 0)
                    + coalesce(s.despesas_pagas_rap, 0)
            end as pago,
            case
                when s.sgac_id is not null
                then greatest(
                    coalesce(s.valor_firmado, p.total_de_recursos, 0)
                    - (
                        coalesce(s.despesas_pagas_exercicio, 0)
                        + coalesce(s.despesas_pagas_rap, 0)
                    ),
                    0
                )
            end as pendente_de_pagamento,
            p.status as situacao,
            p.id as sgac_id,
            (
                p.instrumento ilike '%TED%'
                or p.instrumento ilike '%execução descentralizada%'
                or p.instrumento ilike '%Dispensa de TED%'
            ) as eh_ted
        from projetos as p
        left join teds_siafi as s
            on s.sgac_id = p.id
    )

select *
from relatorio
order by inicio_execucao desc, data_de_conclusao, diretoria, entidades_externas
