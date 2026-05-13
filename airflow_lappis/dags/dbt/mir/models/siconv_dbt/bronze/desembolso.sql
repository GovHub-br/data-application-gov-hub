{{ config(materialized="table") }}

with
    desembolso_raw as (
        select
            nullif(id_desembolso, '')::integer as id_desembolso,
            nullif(nr_convenio, '')::integer as nr_convenio,
            to_date(nullif(dt_ult_desembolso, ''), 'DD/MM/YYYY') as dt_ult_desembolso,
            nullif(qtd_dias_sem_desembolso, '')::integer as qtd_dias_sem_desembolso,
            to_date(nullif(data_desembolso, ''), 'DD/MM/YYYY') as data_desembolso,
            nullif(ano_desembolso, '')::integer as ano_desembolso,
            nullif(mes_desembolso, '')::integer as mes_desembolso,
            nr_siafi::text as nr_siafi,
            nullif(ug_emitente_dh, '')::integer as ug_emitente_dh,
            observacao_dh::text as observacao_dh,
            replace(nullif(vl_desembolsado, ''), ',', '.')::numeric(15, 2) as vl_desembolsado
        from {{ source("siconv", "desembolso") }}
    )

select *
from desembolso_raw