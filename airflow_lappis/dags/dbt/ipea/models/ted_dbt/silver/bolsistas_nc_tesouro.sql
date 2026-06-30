with

    pagamentos as (select * from {{ ref("pagamentos_ob_bolsa") }}),

    -- ponte: OB da bolsa paga -> (fonte de recurso, ptres)
    bolsa_ponte as (
        select distinct
            item_informacao as ob,
            fonte_recursos_codigo,
            ptres
        from {{ ref("bolsas_pagas") }}
        where item_informacao not in ('-8', '-9', '')
    ),

    -- (fonte, ptres) -> número de transferência (TED); só transferências reais
    transferencias as (
        select distinct
            nc_fonte_recursos,
            ptres,
            nc_transferencia
        from {{ ref("nc_tesouro") }}
        where nc_transferencia not in ('-8', '-9', '')
    ),

    -- consolida por OB: TEDs candidatos + fontes (mantém o grão de pagamento no join final)
    ob_ted as (
        select
            b.ob,
            array_agg(distinct b.fonte_recursos_codigo) as fontes_recurso,
            array_agg(distinct t.nc_transferencia) filter (
                where t.nc_transferencia is not null
            ) as nums_transferencia,
            count(distinct t.nc_transferencia) as qtd_transferencia
        from bolsa_ponte b
        left join transferencias t
            on b.fonte_recursos_codigo = t.nc_fonte_recursos
            and b.ptres = t.ptres
        group by b.ob
    )

select
    p.ob,
    p.oblc_sequencial,
    p.dh_dia_pagamento,
    p.oblc_favorecido_numero as bolsista_cpf,
    p.oblc_favorecido_nome as bolsista_nome,
    p.oblc_valor as valor_pago,
    ot.fontes_recurso,
    ot.nums_transferencia,
    coalesce(ot.qtd_transferencia, 0) as qtd_transferencia,
    p.dt_ingest
from pagamentos p
left join ob_ted ot on p.ob = ot.ob
