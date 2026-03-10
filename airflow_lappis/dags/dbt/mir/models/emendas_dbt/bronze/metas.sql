{{ config(materialized="table") }}

with
    meta_especial_raw as (
        select
            id_executor::integer as id_executor,
            id_meta::integer as id_meta,
            sequencial_meta::integer as sequencial_meta,
            nome_meta::text as nome_meta,
            desc_meta::text as desc_meta,
            un_medida_meta::text as un_medida_meta,
            qt_uniade_meta::numeric(15, 2) as qt_uniade_meta,
            vl_custeio_emenda_especial_meta::numeric(15, 2) as valor_custeio_emenda_especial_meta,
            vl_investimento_emenda_especial_meta::numeric(15, 2) as valor_investimento_emenda_especial_meta,
            vl_custeio_recursos_proprios_meta::numeric(15, 2) as valor_custeio_recursos_proprios_meta,
            vl_investimento_recursos_proprios_meta::numeric(15, 2) as valor_investimento_recursos_proprios_meta,
            vl_custeio_rendimento_meta::numeric(15, 2) as valor_custeio_rendimento_meta,
            vl_investimento_rendimento_meta::numeric(15, 2) as valor_investimento_rendimento_meta,
            vl_custeio_doacao_meta::numeric(15, 2) as valor_custeio_doacao_meta,
            vl_investimento_doacao_meta::numeric(15, 2) as valor_investimento_doacao_meta,
            qt_meses_meta::integer as qt_meses_meta,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "metas_especiais") }}
    )

select *
from meta_especial_raw