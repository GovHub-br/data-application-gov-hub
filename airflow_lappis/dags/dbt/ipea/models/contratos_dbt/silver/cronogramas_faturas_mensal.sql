with

    cronograma_agg as (
        select contrato_id, vencimento as mes_ref, sum(valor) as valor_cronograma
        from {{ ref("cronogramas") }}
        group by 1, 2
        order by contrato_id, vencimento
    ),

    faturas_parsed as (
        select
            contrato_id::integer as contrato_id,
            emissao::date as emissao,
            replace(replace(juros::text, '.', ''), ',', '.')::numeric(15, 2) as juros,
            replace(replace(multa::text, '.', ''), ',', '.')::numeric(15, 2) as multa,
            replace(replace(glosa::text, '.', ''), ',', '.')::numeric(15, 2) as glosa,
            replace(replace(valorliquido::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as valorliquido,
            situacao::text as situacao,
            dt_ingest
        from {{ source("compras_gov", "faturas") }}
    ),

    faturas_pago as (
        select
            contrato_id,
            to_date(
                split_part(emissao::text, '-', 1)  -- verificar se o mês de emissão é o adequado para ser utilizada
                || '-'
                || split_part(emissao::text, '-', 2),
                'YYYY-MM'
            ) as mes_ref,
            sum(juros + multa + glosa + valorliquido) as valor_faturas_pagas,
            max(dt_ingest) as dt_ingest_pago
        from faturas_parsed
        where situacao = 'Pago'
        group by 1, 2
    ),

    faturas_pendente as (
        select
            contrato_id,
            to_date(
                split_part(emissao::text, '-', 1)
                || '-'
                || split_part(emissao::text, '-', 2),
                'YYYY-MM'
            ) as mes_ref,
            sum(juros + multa + glosa + valorliquido) as valor_faturas_pendentes,
            max(dt_ingest) as dt_ingest_pendente
        from faturas_parsed
        where situacao = 'Pendente'
        group by 1, 2
    ),

    joined_table as (
        select *
        from cronograma_agg
        left join faturas_pago using (contrato_id, mes_ref)
        left join faturas_pendente using (contrato_id, mes_ref)
    ),

    joined_ajustado as (
        select
            contrato_id::text,
            mes_ref,
            coalesce(valor_cronograma, 0) as valor_cronograma,
            coalesce(valor_faturas_pagas, 0) as valor_faturas_pagas,
            coalesce(valor_faturas_pendentes, 0) as valor_faturas_pendentes,
            coalesce(valor_cronograma, 0)
            - coalesce(valor_faturas_pagas, 0)
            - coalesce(valor_faturas_pendentes, 0) as saldo_contratual_disponivel,
            greatest(dt_ingest_pago, dt_ingest_pendente) AS dt_ingest
        from joined_table
        order by contrato_id, mes_ref
    )

select *
from joined_ajustado
