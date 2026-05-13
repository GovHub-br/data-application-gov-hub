{{ config(materialized="table") }}

with
    prorroga_oficio_raw as (
        select
            nullif(nr_convenio, '')::integer as nr_convenio,
            nr_prorroga::text as nr_prorroga,
            to_date(nullif(dt_inicio_prorroga, ''), 'DD/MM/YYYY') as dt_inicio_prorroga,
            to_date(nullif(dt_fim_prorroga, ''), 'DD/MM/YYYY') as dt_fim_prorroga,
            nullif(dias_prorroga, '')::integer as dias_prorroga,
            to_date(nullif(dt_assinatura_prorroga, ''), 'DD/MM/YYYY') as dt_assinatura_prorroga,
            sit_prorroga::text as sit_prorroga
        from {{ source("siconv", "prorroga_oficio") }}
    )

select *
from prorroga_oficio_raw