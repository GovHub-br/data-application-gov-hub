{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    proposta as (
        select *
        from {{ ref("proposta") }}
    )

select
    c.*,
    p.uf_proponente,
    p.munic_proponente,
    p.cod_munic_ibge,
    p.natureza_juridica,
    p.nr_proposta,
    p.dia_prop,
    p.mes_prop,
    p.ano_prop,
    p.dia_proposta,
    p.cod_orgao,
    p.desc_orgao,
    p.modalidade,
    p.identif_proponente,
    p.nm_proponente,
    p.cep_proponente,
    p.endereco_proponente,
    p.bairro_proponente,
    p.nm_banco,
    p.situacao_conta,
    p.situacao_projeto_basico,
    p.sit_proposta,
    p.dia_inic_vigencia_proposta,
    p.dia_fim_vigencia_proposta,
    p.objeto_proposta,
    p.item_investimento,
    p.enviada_mandataria,
    p.nome_subtipo_proposta,
    p.descricao_subtipo_proposta,
    p.vl_global_prop,
    p.vl_repasse_prop,
    p.vl_contrapartida_prop
from convenio c
left join proposta p on c.id_proposta = p.id_proposta