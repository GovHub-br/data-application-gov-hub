{{ config(materialized="view") }}

with

    nc_transfere_gov as (
        select distinct id_plano_acao, tx_numero_nota as nc, ndc.cd_ug_emitente_nota as ug
        from {{ source("transfere_gov", "notas_de_credito") }} ndc
        where ndc.tx_numero_nota is not null
    ),

    nc_siafi as (
        select distinct
            left(nc, 6) as ug, right(nc, 12) as nc, nt.nc_transferencia as num_transf
        from {{ref("nc_tesouro_mir")}} nt
        where nc_transferencia != '-8'
    ),

    joined as (
        select distinct num_transf, id_plano_acao as plano_acao
        from nc_siafi
        left join nc_transfere_gov using (nc, ug)
    ),

    ranked as (
        select
            *,
            row_number() over (
                partition by num_transf
                order by case when plano_acao is not null then 1 else 2 end
            ) as rn
        from joined
    ),

    via_nc as (
        select num_transf, plano_acao
        from ranked
        where rn = 1
    ),

    via_sq_instrumento as (
        select
            sq_instrumento as num_transf,
            id_plano_acao::text as plano_acao
        from {{ ref("planos_acao_ted") }}
        where sq_instrumento is not null
    ),

    unificado as (
        select * from via_nc
        union
        select * from via_sq_instrumento
    ),

    final as (
        select
            *,
            row_number() over (
                partition by num_transf
                order by case when plano_acao is not null then 1 else 2 end
            ) as rn
        from unificado
    )

select num_transf, plano_acao
from final
where rn = 1
