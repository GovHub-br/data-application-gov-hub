with

    programacoes_financeira as (
        select
            {{ target.schema }}.parse_date(emissao_mes) as emissao_mes,
            to_date(emissao_dia, 'DD/MM/YYYY') as emissao_dia,
            ug_emitente,
            ug_emitente_descricao,
            ug_favorecido,
            ug_favorecido_descricao,
            pf_evento,
            pf_evento_descricao,
            right(pf, 12) as pf,
            pf_inscricao,
            pf_acao,
            pf_acao_descricao,
            pf_fonte_recursos,
            pf_fonte_recursos_descricao,
            pf_vinculacao_pagamento,
            pf_vinculacao_pagamento_descricao,
            pf_categoria_gasto,
            pf_recurso,
            pf_recurso_descricao,
            doc_observacao,
            replace(pf_valor_linha, ',', '.')::numeric(15, 2) as pf_valor_linha
        from {{ source("siafi", "pf_tesouro") }}
    )

--
select *
from programacoes_financeira
