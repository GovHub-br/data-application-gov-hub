{{ config(materialized="table") }}

with
    finalidade_especial_raw as (
        select
            id_executor::integer as id_executor,
            cd_area_politica_publica_tipo_pt::integer as cd_area_politica_publica_tipo_pt,
            area_politica_publica_tipo_pt::text as area_politica_publica_tipo_pt,
            cd_area_politica_publica_pt::integer as cd_area_politica_publica_pt,
            area_politica_publica_pt::text as area_politica_publica_pt,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "finalidades_especiais") }}
    )

select *
from finalidade_especial_raw