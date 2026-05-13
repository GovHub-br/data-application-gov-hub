{{ config(materialized="table") }}

with
    pagamento_raw as (
        select
            nullif(nr_mov_fin, '')::integer as nr_mov_fin,
            nullif(nr_convenio, '')::integer as nr_convenio,
            identif_fornecedor::text as identif_fornecedor,
            nome_fornecedor::text as nome_fornecedor,
            tp_mov_financeira::text as tp_mov_financeira,
            case
                when nullif(data_pag, '') is null then null
                when data_pag ~ '^\d{2}/\d{2}/\d{4}$' then to_date(data_pag, 'DD/MM/YYYY')
                else to_date(data_pag, 'YYYY-MM-DD')
            end as data_pag,
            nr_dl::text as nr_dl,
            desc_dl::text as desc_dl,
            replace(nullif(vl_pago, ''), ',', '.')::numeric(15, 2) as vl_pago,
            nullif(id_dl, '')::integer as id_dl,
            case
                when nullif(data_emissao_dl, '') is null then null
                when data_emissao_dl ~ '^\d{2}/\d{2}/\d{4}$' then to_date(data_emissao_dl, 'DD/MM/YYYY')
                else to_date(data_emissao_dl, 'YYYY-MM-DD')
            end as data_emissao_dl
        from {{ source("siconv", "pagamento") }}
    )

select *
from pagamento_raw