{{ config(materialized="table") }}

with
    empenho_raw as (
        select
            nullif(id_empenho, '')::integer as id_empenho,
            nullif(nr_convenio, '')::integer as nr_convenio,
            nr_empenho::text as nr_empenho,
            tipo_nota::text as tipo_nota,
            desc_tipo_nota::text as desc_tipo_nota,
            to_date(nullif(data_emissao, ''), 'DD/MM/YYYY') as data_emissao,
            cod_situacao_empenho::text as cod_situacao_empenho,
            desc_situacao_empenho::text as desc_situacao_empenho,
            nullif(ug_emitente, '')::integer as ug_emitente,
            nullif(ug_responsavel, '')::integer as ug_responsavel,
            fonte_recurso::text as fonte_recurso,
            natureza_despesa::text as natureza_despesa,
            plano_interno::text as plano_interno,
            ptres::text as ptres,
            replace(nullif(valor_empenho, ''), ',', '.')::numeric(15, 2) as valor_empenho,
            resultado_primario::text as resultado_primario,
            observacao_empenho::text as observacao_empenho,
            descricao_emenda_siafi::text as descricao_emenda_siafi
        from {{ source("siconv", "empenho") }}
    )

select *
from empenho_raw