with

    pagamentos_ob_bolsa as (
        select
            {{ target.schema }}.parse_date(dh_mes_emissao) as dh_mes_emissao,
            to_date(nullif(dh_dia_emissao, ''), 'DD/MM/YYYY') as dh_dia_emissao,
            to_date(nullif(dh_dia_pagamento, ''), 'DD/MM/YYYY') as dh_dia_pagamento,
            emitente_ug_codigo,
            emitente_ug_nome,
            ob,
            ob_lista_credores,
            oblc_favorecido_numero,
            oblc_favorecido_nome,
            oblc_banco_codigo,
            oblc_banco_nome,
            oblc_agencia_bancaria_codigo,
            oblc_agencia_bancaria_nome,
            oblc_conta_bancaria,
            {{ parse_financial_value("oblc_valor") }} as oblc_valor,
            oblc_sequencial,
            ob_processo,
            documento_habil,
            doc_observacao,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("siafi", "pagamentos_ob_bolsa") }}
    )

--
select *
from pagamentos_ob_bolsa
