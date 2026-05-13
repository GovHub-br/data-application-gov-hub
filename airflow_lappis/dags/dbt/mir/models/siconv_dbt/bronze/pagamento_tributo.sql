{{ config(materialized="table") }}

with
    pagamento_tributo_raw as (
        select
            nr_convenio::integer as nr_convenio,
            to_date(nullif(data_tributo, ''), 'DD/MM/YYYY') as data_tributo,
            replace(nullif(vl_pag_tributos, ''), ',', '.')::numeric(15, 2) as vl_pag_tributos
        from {{ source("siconv", "pagamento_tributo") }}
    )

select *
from pagamento_tributo_raw