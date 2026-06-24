{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    pagamento as (
        select *
        from {{ ref("pagamento") }}
    )

select
    c.*,
    p.nr_mov_fin,
    p.identif_fornecedor,
    p.nome_fornecedor,
    p.tp_mov_financeira,
    p.data_pag,
    p.nr_dl,
    p.desc_dl,
    p.vl_pago,
    p.id_dl,
    p.data_emissao_dl
from convenio c
left join pagamento p on c.nr_convenio = p.nr_convenio