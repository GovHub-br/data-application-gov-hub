{{ config(materialized="table") }}

with
    desbloqueio_raw as (
        select
            nr_convenio::integer as nr_convenio,
            nr_ob::text as nr_ob,
            to_date(nullif(data_cadastro, ''), 'DD/MM/YYYY') as data_cadastro,
            to_date(nullif(data_envio, ''), 'DD/MM/YYYY') as data_envio,
            tipo_recurso_desbloqueio::text as tipo_recurso_desbloqueio,
            replace(nullif(vl_total_desbloqueio, ''), ',', '.')::numeric(15, 2) as vl_total_desbloqueio,
            replace(nullif(vl_desbloqueado, ''), ',', '.')::numeric(15, 2) as vl_desbloqueado,
            replace(nullif(vl_bloqueado, ''), ',', '.')::numeric(15, 2) as vl_bloqueado
        from {{ source("siconv", "desbloqueio") }}
    )

select *
from desbloqueio_raw