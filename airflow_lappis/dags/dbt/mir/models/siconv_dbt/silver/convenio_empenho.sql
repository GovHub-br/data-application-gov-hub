{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    empenho as (
        select *
        from {{ ref("empenho") }}
    )

select
    c.*,
    e.id_empenho,
    e.nr_empenho,
    e.tipo_nota,
    e.desc_tipo_nota,
    e.data_emissao,
    e.cod_situacao_empenho,
    e.desc_situacao_empenho,
    e.ug_emitente as ug_emitente_empenho,
    e.ug_responsavel,
    e.fonte_recurso,
    e.natureza_despesa,
    e.plano_interno,
    e.ptres,
    e.valor_empenho,
    e.resultado_primario,
    e.observacao_empenho,
    e.descricao_emenda_siafi
from convenio c
left join empenho e on c.nr_convenio = e.nr_convenio