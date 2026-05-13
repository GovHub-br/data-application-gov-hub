{{ config(materialized="table") }}

with
    meta_crono_fisico_raw as (
        select
            nullif(id_meta, '')::integer as id_meta,
            nullif(id_proposta, '')::integer as id_proposta,
            nullif(nr_convenio, '')::integer as nr_convenio,
            cod_programa::text as cod_programa,
            nome_programa::text as nome_programa,
            nullif(nr_meta, '')::integer as nr_meta,
            tipo_meta::text as tipo_meta,
            desc_meta::text as desc_meta,
            to_date(nullif(data_inicio_meta, ''), 'DD/MM/YYYY') as data_inicio_meta,
            to_date(nullif(data_fim_meta, ''), 'DD/MM/YYYY') as data_fim_meta,
            uf_meta::text as uf_meta,
            municipio_meta::text as municipio_meta,
            endereco_meta::text as endereco_meta,
            cep_meta::text as cep_meta,
            replace(nullif(qtd_meta, ''), ',', '.')::numeric(15, 2) as qtd_meta,
            und_fornecimento_meta::text as und_fornecimento_meta,
            replace(nullif(vl_meta, ''), ',', '.')::numeric(15, 2) as vl_meta
        from {{ source("siconv", "meta_crono_fisico") }}
    )

select *
from meta_crono_fisico_raw 