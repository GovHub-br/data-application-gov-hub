{{ config(materialized="view") }}

with

    nc_transfere_gov as (
        select distinct
            ndc.id_plano_acao, ndc.tx_numero_nota as nc, ndc.cd_ug_emitente_nota as ug
        from {{ source("transfere_gov", "notas_de_credito") }} as ndc
        where ndc.tx_numero_nota is not null
    ),

    nc_siafi as (
        select distinct
            nt.nc_transferencia as num_transf, left(nc, 6) as ug, right(nc, 12) as nc
        from {{ source("siafi", "nc_tesouro") }} as nt
        where nt.nc_transferencia != '-8'
    ),

    joined as (
        select distinct num_transf, id_plano_acao as plano_acao
        from nc_siafi
        left join
            nc_transfere_gov
            on nc_siafi.nc = nc_transfere_gov.nc
            and nc_siafi.ug = nc_transfere_gov.ug
    ),

    ranked as (
        select
            *,
            row_number() over (
                partition by num_transf
                order by case when plano_acao is not null then 1 else 2 end
            ) as rn
        from joined
    )

select num_transf, plano_acao
from ranked
where rn = 1
