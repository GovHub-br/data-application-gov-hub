{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    proposta as (
        select *
        from {{ ref("proposta") }}
        where modalidade = 'TERMO DE FOMENTO'
    ),
    empenho_agg as (
        select
            nr_convenio,
            count(*) as qtd_empenhos,
            sum(valor_empenho) as vl_total_empenhos,
            min(data_emissao) as data_primeiro_empenho,
            max(data_emissao) as data_ultimo_empenho
        from {{ ref("empenho") }}
        group by nr_convenio
    ),
    desembolso_agg as (
        select
            nr_convenio,
            count(*) as qtd_desembolsos,
            sum(vl_desembolsado) as vl_total_desembolsos,
            min(data_desembolso) as data_primeiro_desembolso,
            max(data_desembolso) as data_ultimo_desembolso,
            max(qtd_dias_sem_desembolso) as max_dias_sem_desembolso
        from {{ ref("desembolso") }}
        group by nr_convenio
    ),
    pagamento_agg as (
        select
            nr_convenio,
            count(*) as qtd_pagamentos,
            sum(vl_pago) as vl_total_pagamentos,
            min(data_pag) as data_primeiro_pagamento,
            max(data_pag) as data_ultimo_pagamento
        from {{ ref("pagamento") }}
        group by nr_convenio
    ),
    meta_programa as (
        select distinct
            nr_convenio,
            cod_programa as cod_programa_meta,
            nome_programa as nome_programa_meta
        from {{ ref("meta_crono_fisico") }}
        where cod_programa is not null
    ),
    ultima_situacao as (
        select
            nr_convenio,
            dia_historico_sit as data_ultima_situacao,
            historico_sit as ultima_situacao_desc,
            cod_historico_sit as cod_ultima_situacao
        from (
            select
                nr_convenio,
                dia_historico_sit,
                historico_sit,
                cod_historico_sit,
                row_number() over (
                    partition by nr_convenio
                    order by dia_historico_sit desc
                ) as rn
            from {{ ref("historico_situacao") }}
        ) ranked
        where rn = 1
    )

select
    -- Dados do instrumento (convênio)
    c.nr_convenio,
    c.id_proposta,
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

    -- Valores financeiros do instrumento
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

    -- Dados do proponente (proposta)
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
    p.vl_contrapartida_prop,

    -- Enriquecimento: empenhos agregados
    coalesce(ea.qtd_empenhos, 0) as qtd_empenhos,
    coalesce(ea.vl_total_empenhos, 0) as vl_total_empenhos,
    ea.data_primeiro_empenho,
    ea.data_ultimo_empenho,

    -- Enriquecimento: desembolsos agregados
    coalesce(da.qtd_desembolsos, 0) as qtd_desembolsos,
    coalesce(da.vl_total_desembolsos, 0) as vl_total_desembolsos,
    da.data_primeiro_desembolso,
    da.data_ultimo_desembolso,
    da.max_dias_sem_desembolso,

    -- Enriquecimento: pagamentos agregados
    coalesce(pa.qtd_pagamentos, 0) as qtd_pagamentos,
    coalesce(pa.vl_total_pagamentos, 0) as vl_total_pagamentos,
    pa.data_primeiro_pagamento,
    pa.data_ultimo_pagamento,

    -- Enriquecimento: programa associado
    mp.cod_programa_meta,
    mp.nome_programa_meta,

    -- Enriquecimento: última situação registrada
    us.data_ultima_situacao,
    us.ultima_situacao_desc,
    us.cod_ultima_situacao

from convenio c
inner join proposta p on c.id_proposta = p.id_proposta
left join empenho_agg ea on c.nr_convenio = ea.nr_convenio
left join desembolso_agg da on c.nr_convenio = da.nr_convenio
left join pagamento_agg pa on c.nr_convenio = pa.nr_convenio
left join meta_programa mp on c.nr_convenio = mp.nr_convenio
left join ultima_situacao us on c.nr_convenio = us.nr_convenio
