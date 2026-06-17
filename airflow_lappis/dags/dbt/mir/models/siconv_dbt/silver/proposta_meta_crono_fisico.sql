{{ config(materialized="table") }}

with
    proposta as (
        select *
        from {{ ref("proposta") }}
    ),
    meta_crono_fisico as (
        select *
        from {{ ref("meta_crono_fisico") }}
    )

select
    p.*,
    m.id_meta,
    m.nr_convenio as nr_convenio_meta,
    m.nr_meta,
    m.tipo_meta,
    m.desc_meta,
    m.data_inicio_meta,
    m.data_fim_meta,
    m.uf_meta,
    m.municipio_meta,
    m.endereco_meta,
    m.cep_meta,
    m.qtd_meta,
    m.und_fornecimento_meta,
    m.vl_meta,
    m.cod_programa,
    m.nome_programa
from proposta p
left join meta_crono_fisico m on p.id_proposta = m.id_proposta