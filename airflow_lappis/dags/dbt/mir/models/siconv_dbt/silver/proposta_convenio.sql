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
    -- Colunas do convênio
    c.nr_convenio,
    c.dia as dia_conv,
    c.mes as mes_conv,
    c.ano as ano_conv,
    c.dia_assin_conv,
    c.sit_convenio,
    c.subsituacao_conv,
    c.situacao_publicacao,
    c.instrumento_ativo,
    c.ind_opera_obtv,
    c.nr_processo,
    c.ug_emitente,
    c.dia_publ_conv,
    c.dia_inic_vigenc_conv,
    c.dia_fim_vigenc_conv,
    c.dia_fim_vigenc_original_conv,
    c.dias_prest_contas,
    c.dia_limite_prest_contas,
    c.data_suspensiva,
    c.data_retirada_suspensiva,
    c.dias_clausula_suspensiva,
    c.situacao_contratacao,
    c.ind_assinado,
    c.motivo_suspensao,
    c.ind_foto,
    c.qtde_convenios,
    c.qtd_ta,
    c.qtd_prorroga,
    c.vl_global_conv,
    c.vl_repasse_conv,
    c.vl_contrapartida_conv,
    c.vl_empenhado_conv,
    c.vl_desembolsado_conv,
    c.vl_saldo_reman_tesouro,
    c.vl_saldo_reman_convenente,
    c.vl_rendimento_aplicacao,
    c.vl_ingresso_contrapartida,
    c.vl_saldo_conta,
    c.valor_global_original_conv,

    -- Colunas da proposta
    p.id_proposta,
    p.uf_proponente,
    p.munic_proponente,
    p.cod_munic_ibge,
    p.cod_orgao_sup,
    p.desc_orgao_sup,
    p.natureza_juridica,
    p.nr_proposta,
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
inner join proposta p on c.id_proposta = p.id_proposta
where p.modalidade in ('CONVENIO', 'TERMO DE FOMENTO')
