with

    pagamentos as (select * from {{ ref("pagamentos_ob_bolsa") }}),

    -- atributos orçamentários da bolsa paga, consolidados por OB (item_informacao)
    bolsa_attrs as (
        select
            item_informacao as ob,
            array_agg(distinct fonte_recursos_codigo) filter (
                where fonte_recursos_codigo is not null and fonte_recursos_codigo <> ''
            ) as fontes_recurso,
            array_agg(distinct ptres) filter (
                where ptres is not null and ptres <> ''
            ) as ptres_list,
            array_agg(distinct ne_ccor) filter (
                where ne_ccor is not null and ne_ccor <> ''
            ) as empenhos,
            max(fonte_recursos_descricao) as fonte_recursos_descricao,
            max(natureza_codigo) as natureza_codigo,
            max(natureza_descricao) as natureza_descricao,
            max(pi_codigo) as pi_codigo,
            max(pi_descricao) as pi_descricao,
            max(processo) as processo_bolsa,
            max(observacao) as observacao_bolsa
        from {{ ref("bolsas_pagas") }}
        where item_informacao not in ('-8', '-9', '')
        group by 1
    ),

    -- (fonte, ptres) -> número de transferência (TED); só transferências reais
    transferencias as (
        select distinct nc_fonte_recursos, ptres, nc_transferencia
        from {{ ref("nc_tesouro") }}
        where nc_transferencia not in ('-8', '-9', '')
    ),

    -- ponte por (fonte, ptres) consolidada por OB -> TEDs candidatos
    ob_ted as (
        select
            b.ob,
            array_agg(distinct t.nc_transferencia) filter (
                where t.nc_transferencia is not null
            ) as nums_transferencia,
            count(distinct t.nc_transferencia) as qtd_transferencia
        from (
            select distinct item_informacao as ob, fonte_recursos_codigo, ptres
            from {{ ref("bolsas_pagas") }}
            where item_informacao not in ('-8', '-9', '')
        ) b
        left join transferencias t
            on b.fonte_recursos_codigo = t.nc_fonte_recursos
            and b.ptres = t.ptres
        group by b.ob
    ),

    -- contexto (representativo) da nota de crédito por número de transferência
    nc_resumo as (
        select
            nc_transferencia,
            max(nc_fonte_recursos_descricao) as ted_fonte_descricao,
            max(ug_responsavel_descricao) as ted_ug_responsavel,
            max(plano_detalhado_descricao2) as ted_objeto
        from {{ ref("nc_tesouro") }}
        where nc_transferencia not in ('-8', '-9', '')
        group by 1
    ),

    base as (
        select
            -- identidade do pagamento (grão)
            p.ob,
            p.oblc_sequencial,
            p.ob_lista_credores,
            p.documento_habil,
            -- datas
            p.dh_mes_emissao,
            p.dh_dia_emissao,
            p.dh_dia_pagamento,
            -- unidade gestora emitente
            p.emitente_ug_codigo,
            p.emitente_ug_nome,
            -- bolsista
            p.oblc_favorecido_numero as bolsista_cpf,
            p.oblc_favorecido_nome as bolsista_nome,
            -- dados bancários
            p.oblc_banco_codigo,
            p.oblc_banco_nome,
            p.oblc_agencia_bancaria_codigo,
            p.oblc_agencia_bancaria_nome,
            p.oblc_conta_bancaria,
            -- valor e referências do pagamento
            p.oblc_valor as valor_pago,
            p.ob_processo,
            p.doc_observacao as observacao_pagamento,
            -- classificação orçamentária da bolsa (por OB)
            ba.fontes_recurso,
            ba.fonte_recursos_descricao,
            ba.ptres_list,
            ba.natureza_codigo,
            ba.natureza_descricao,
            ba.pi_codigo,
            ba.pi_descricao,
            ba.empenhos,
            ba.processo_bolsa,
            ba.observacao_bolsa,
            -- TED de origem: resolvido só quando o vínculo é único
            case
                when ot.qtd_transferencia = 1 then ot.nums_transferencia[1]
            end as num_transferencia,
            coalesce(ot.qtd_transferencia, 0) as qtd_transferencia,
            p.dt_ingest
        from pagamentos p
        left join bolsa_attrs ba on p.ob = ba.ob
        left join ob_ted ot on p.ob = ot.ob
    )

select
    base.*,
    -- contexto do TED (preenchido quando num_transferencia está resolvido)
    nr.ted_fonte_descricao,
    nr.ted_ug_responsavel,
    nr.ted_objeto
from base
left join nc_resumo nr on base.num_transferencia = nr.nc_transferencia
