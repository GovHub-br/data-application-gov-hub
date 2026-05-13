{{ config(materialized="table") }}

with
    termo_aditivo_raw as (
        select
            nullif(nr_convenio, '')::integer as nr_convenio,
            nullif(id_solicitacao, '')::integer as id_solicitacao,
            numero_ta::text as numero_ta,
            tipo_ta::text as tipo_ta,
            replace(nullif(vl_global_ta, ''), ',', '.')::numeric(15, 2) as vl_global_ta,
            replace(nullif(vl_repasse_ta, ''), ',', '.')::numeric(15, 2) as vl_repasse_ta,
            replace(nullif(vl_contrapartida_ta, ''), ',', '.')::numeric(15, 2) as vl_contrapartida_ta,
            to_date(nullif(dt_assinatura_ta, ''), 'DD/MM/YYYY') as dt_assinatura_ta,
            to_date(nullif(dt_inicio_ta, ''), 'DD/MM/YYYY') as dt_inicio_ta,
            to_date(nullif(dt_fim_ta, ''), 'DD/MM/YYYY') as dt_fim_ta,
            justificativa_ta::text as justificativa_ta
        from {{ source("siconv", "termo_aditivo") }}
    )

select *
from termo_aditivo_raw