{{ config(materialized="table") }}

with
    historico_situacao_raw as (
        select
            nullif(id_proposta, '')::integer as id_proposta,
            nullif(nr_convenio, '')::integer as nr_convenio,
            to_date(nullif(dia_historico_sit, ''), 'DD/MM/YYYY') as dia_historico_sit,
            historico_sit::text as historico_sit,
            nullif(dias_historico_sit, '')::integer as dias_historico_sit,
            nullif(cod_historico_sit, '')::integer as cod_historico_sit
        from {{ source("siconv", "historico_situacao") }}
    )

select *
from historico_situacao_raw