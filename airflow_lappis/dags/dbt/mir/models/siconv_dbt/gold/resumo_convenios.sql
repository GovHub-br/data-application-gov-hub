{{ config(materialized="table") }}

with
    base as (
        select *
        from {{ ref("convenios_consolidados") }}
    ),
    proposta as (
        select
            id_proposta,
            uf_proponente,
            munic_proponente,
            modalidade,
            nm_proponente,
            natureza_juridica,
            objeto_proposta
        from {{ ref("proposta") }}
    ),
    desembolso as (
        select
            nr_convenio,
            sum(vl_desembolsado) as valor_total_repassado,
            count(id_desembolso) as quantidade_desembolsos,
            max(data_desembolso) as data_ultimo_desembolso,
            max(qtd_dias_sem_desembolso) as max_dias_sem_repasse
        from {{ ref("desembolso") }}
        group by nr_convenio
    ),
    desbloqueio as (
        select
            nr_convenio,
            sum(vl_desbloqueado) as valor_desbloqueado,
            sum(vl_bloqueado) as valor_bloqueado,
            max(data_cadastro) as data_ultimo_desbloqueio
        from {{ ref("desbloqueio") }}
        group by nr_convenio
    ),
    empenho as (
        select
            nr_convenio,
            sum(valor_empenho) as valor_empenhado,
            count(id_empenho) as quantidade_empenhos
        from {{ ref("empenho") }}
        group by nr_convenio
    ),
    licitacao as (
        select
            nr_convenio,
            count(id_licitacao) as quantidade_licitacoes,
            sum(valor_licitacao) as valor_total_contratado
        from {{ ref("licitacao") }}
        group by nr_convenio
    ),
    pagamento as (
        select
            nr_convenio,
            count(id_dl) as quantidade_pagamentos,
            sum(vl_pago) as valor_total_pago,
            max(data_pag) as data_ultimo_pagamento
        from {{ ref("pagamento") }}
        group by nr_convenio
    ),
    pagamento_tributo as (
        select
            nr_convenio,
            sum(vl_pag_tributos) as valor_total_tributos,
            count(*) as quantidade_pagamentos_tributo,
            max(data_tributo) as data_ultimo_tributo
        from {{ ref("pagamento_tributo") }}
        group by nr_convenio
    ),
    ingresso_contrapartida as (
        select
            nr_convenio,
            sum(vl_ingresso_contrapartida) as valor_contrapartida_depositado
        from {{ ref("ingresso_contrapartida") }}
        group by nr_convenio
    ),
    historico as (
        select
            nr_convenio,
            count(*) as quantidade_mudancas_situacao,
            bool_or(historico_sit = 'INADIMPLENTE') as inadimplente,
            bool_or(historico_sit = 'CONVENIO_RESCINDIDO') as rescindido,
            bool_or(historico_sit = 'CONVENIO_ANULADO') as anulado
        from {{ ref("historico_situacao") }}
        group by nr_convenio
    ),
    meta as (
        select
            nr_convenio,
            count(id_meta) as quantidade_metas,
            bool_or(data_fim_meta < current_date) as prazo_meta_expirado
        from {{ ref("meta_crono_fisico") }}
        group by nr_convenio
    ),
    prorroga as (
        select
            nr_convenio,
            count(nr_prorroga) as quantidade_prorrogacoes,
            sum(dias_prorroga) as total_dias_prorrogado
        from {{ ref("prorroga_oficio") }}
        group by nr_convenio
    ),
    solicitacao_alteracao as (
        select
            nr_convenio,
            count(id_solicitacao) as quantidade_solicitacoes_alteracao
        from {{ ref("solicitacao_alteracao") }}
        group by nr_convenio
    ),
    solicitacao_rendimento as (
        select
            nr_convenio,
            sum(valor_solicitacao_rend_aplicacao) as valor_solicitado_rendimento_aplicacao,
            sum(valor_aprovado_solicitacao_rend_aplicacao) as valor_aprovado_rendimento_aplicacao
        from {{ ref("solicitacao_rendimento_aplicacao") }}
        group by nr_convenio
    ),
    termo_aditivo as (
        select
            nr_convenio,
            count(numero_ta) as quantidade_aditivos,
            sum(vl_global_ta) as valor_total_aditivado,
            max(dt_fim_ta) as novo_prazo_vigencia
        from {{ ref("termo_aditivo") }}
        group by nr_convenio
    ),
    cronograma as (
        select
            nr_convenio,
            sum(valor_parcela_crono_desembolso) as valor_total_previsto,
            sum(case when tipo_resp_crono_desembolso = 'Concedente' then valor_parcela_crono_desembolso else 0 end) as valor_previsto_concedente,
            sum(case when tipo_resp_crono_desembolso = 'Convenente' then valor_parcela_crono_desembolso else 0 end) as valor_previsto_convenente,
            sum(case when tipo_resp_crono_desembolso = 'Rendimento de Aplicação' then valor_parcela_crono_desembolso else 0 end) as valor_previsto_rendimento,
            count(nr_parcela_crono_desembolso) as quantidade_parcelas
        from {{ ref("cronograma_desembolso") }}
        group by nr_convenio
    ),
    emendas as (
        select
            nr_convenio,
            string_agg(distinct autor, ', ') as parlamentares,
            string_agg(distinct partido, ', ') as partidos,
            string_agg(distinct uf_autor, ', ') as ufs_parlamentares
        from {{ ref("emendas_convenio") }}
        where nr_convenio is not null
        group by nr_convenio
    )

select
    -- Identificação
    b.nr_convenio,
    p.modalidade as modalidade_instrumento,
    p.uf_proponente as uf_execucao,
    p.munic_proponente as municipio_execucao,
    p.nm_proponente as nome_convenente,
    p.natureza_juridica as categoria_convenente,
    p.objeto_proposta as objeto,

    -- Valores
    b.vl_global_conv as valor_firmado_atualizado,
    b.valor_global_original_conv as valor_firmado_inicial,
    b.vl_repasse_conv as valor_repasse_previsto,
    b.vl_desembolsado_conv as valor_repassado,
    b.vl_saldo_conta as saldo_disponivel,
    b.vl_contrapartida_conv as valor_contrapartida_previsto,

    -- Situação
    b.sit_convenio as situacao_atual,
    b.dia_assin_conv as data_assinatura,
    b.dia_inic_vigenc_conv as inicio_vigencia,
    b.dia_fim_vigenc_conv as fim_vigencia,
    case when b.dia_fim_vigenc_conv >= current_date then true else false end as prazo_vigente,
    h.inadimplente,
    h.rescindido,
    h.anulado,
    h.quantidade_mudancas_situacao,

    -- Execução financeira
    d.valor_total_repassado,
    d.quantidade_desembolsos,
    d.data_ultimo_desembolso,
    d.max_dias_sem_repasse,
    e.valor_empenhado,
    e.quantidade_empenhos,
    pg.valor_total_pago,
    pg.quantidade_pagamentos,
    pg.data_ultimo_pagamento,

    -- Desbloqueio
    db.valor_desbloqueado,
    db.valor_bloqueado,
    db.data_ultimo_desbloqueio,

    -- Tributos
    pt.valor_total_tributos,
    pt.quantidade_pagamentos_tributo,

    -- Contrapartida
    ic.valor_contrapartida_depositado,

    -- Licitações
    l.quantidade_licitacoes,
    l.valor_total_contratado,

    -- Metas físicas
    m.quantidade_metas,
    m.prazo_meta_expirado,

    -- Aditivos
    ta.quantidade_aditivos,
    ta.valor_total_aditivado,
    ta.novo_prazo_vigencia,

    -- Prorrogação
    pr.quantidade_prorrogacoes,
    pr.total_dias_prorrogado,

    -- Solicitações
    sa.quantidade_solicitacoes_alteracao,
    sr.valor_solicitado_rendimento_aplicacao,
    sr.valor_aprovado_rendimento_aplicacao,

    -- Cronograma
    cr.valor_total_previsto,
    cr.valor_previsto_concedente,
    cr.valor_previsto_convenente,
    cr.valor_previsto_rendimento,
    cr.quantidade_parcelas,

    -- Emendas
    em.parlamentares,
    em.partidos,
    em.ufs_parlamentares

from base b
left join proposta p on b.id_proposta = p.id_proposta
left join desembolso d on b.nr_convenio = d.nr_convenio
left join desbloqueio db on b.nr_convenio = db.nr_convenio
left join empenho e on b.nr_convenio = e.nr_convenio
left join licitacao l on b.nr_convenio = l.nr_convenio
left join pagamento pg on b.nr_convenio = pg.nr_convenio
left join pagamento_tributo pt on b.nr_convenio = pt.nr_convenio
left join ingresso_contrapartida ic on b.nr_convenio = ic.nr_convenio
left join historico h on b.nr_convenio = h.nr_convenio
left join meta m on b.nr_convenio = m.nr_convenio
left join prorroga pr on b.nr_convenio = pr.nr_convenio
left join solicitacao_alteracao sa on b.nr_convenio = sa.nr_convenio
left join solicitacao_rendimento sr on b.nr_convenio = sr.nr_convenio
left join termo_aditivo ta on b.nr_convenio = ta.nr_convenio
left join cronograma cr on b.nr_convenio = cr.nr_convenio
left join emendas em on b.nr_convenio = em.nr_convenio