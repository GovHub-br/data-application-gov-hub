{{ config(materialized="table") }}

with
    termo_fomento as (
        select *
        from {{ ref("termo_fomento") }}
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
    -- Dados base do termo de fomento (silver)
    tf.nr_convenio,
    tf.id_proposta,
    tf.dia_conv,
    tf.mes_conv,
    tf.ano_conv,
    tf.dia_assin_conv,
    tf.sit_convenio,
    tf.subsituacao_conv,
    tf.situacao_publicacao,
    tf.instrumento_ativo,
    tf.ind_opera_obtv,
    tf.nr_processo,
    tf.ug_emitente,
    tf.dia_publ_conv,
    tf.dia_inic_vigenc_conv,
    tf.dia_fim_vigenc_conv,
    tf.dia_fim_vigenc_original_conv,
    tf.dias_prest_contas,
    tf.dia_limite_prest_contas,
    tf.data_suspensiva,
    tf.data_retirada_suspensiva,
    tf.dias_clausula_suspensiva,
    tf.situacao_contratacao,
    tf.ind_assinado,
    tf.motivo_suspensao,
    tf.ind_foto,
    tf.qtde_convenios,
    tf.qtd_ta,
    tf.qtd_prorroga,

    -- Valores financeiros do instrumento
    tf.vl_global_conv,
    tf.vl_repasse_conv,
    tf.vl_contrapartida_conv,
    tf.vl_empenhado_conv,
    tf.vl_desembolsado_conv,
    tf.vl_saldo_reman_tesouro,
    tf.vl_saldo_reman_convenente,
    tf.vl_rendimento_aplicacao,
    tf.vl_ingresso_contrapartida,
    tf.vl_saldo_conta,
    tf.valor_global_original_conv,

    -- Dados do proponente (proposta)
    tf.uf_proponente,
    tf.munic_proponente,
    tf.cod_munic_ibge,
    tf.cod_orgao_sup,
    tf.desc_orgao_sup,
    tf.natureza_juridica,
    tf.nr_proposta,
    tf.dia_proposta,
    tf.cod_orgao,
    tf.desc_orgao,
    tf.modalidade,
    tf.identif_proponente,
    tf.nm_proponente,
    tf.cep_proponente,
    tf.endereco_proponente,
    tf.bairro_proponente,
    tf.nm_banco,
    tf.situacao_conta,
    tf.situacao_projeto_basico,
    tf.sit_proposta,
    tf.dia_inic_vigencia_proposta,
    tf.dia_fim_vigencia_proposta,
    tf.objeto_proposta,
    tf.item_investimento,
    tf.enviada_mandataria,
    tf.nome_subtipo_proposta,
    tf.descricao_subtipo_proposta,
    tf.vl_global_prop,
    tf.vl_repasse_prop,
    tf.vl_contrapartida_prop,

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

from termo_fomento tf
left join empenho_agg ea on tf.nr_convenio = ea.nr_convenio
left join desembolso_agg da on tf.nr_convenio = da.nr_convenio
left join pagamento_agg pa on tf.nr_convenio = pa.nr_convenio
left join meta_programa mp on tf.nr_convenio = mp.nr_convenio
left join ultima_situacao us on tf.nr_convenio = us.nr_convenio
