with

    raw_data as (select * from {{ ref("nc_tesouro") }} nt where nc_transferencia != '-8'),

    planos_de_acao as (
        select distinct *
        from {{ ref("num_transf_n_plano_acao") }}
        where plano_acao is not null
    ),

    result_table as (
        select
            pda.plano_acao,
            emissao_dia,
            nc_transferencia,
            right(nc, 12) as nc,
            nc_fonte_recursos,
            ptres,
            left(nc, 6) as ug_emitente,
            nc_natureza_despesa,
            nc_evento,
            nc_evento_descr,
            -- aplica o sinal correto a depender do tipo de evento
            case
                when nc_evento in ('300302', '300308', '300311', '300083')
                then (-1) * nc_valor_linha
                else nc_valor_linha
            end as nc_valor
        from raw_data rd
        left join planos_de_acao pda on rd.nc_transferencia = pda.num_transf
    )

--
select *
from result_table
order by 1, 2
