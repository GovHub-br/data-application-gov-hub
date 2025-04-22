{{ config(materialized="table") }}

with
    empenhos as (
        select
            id::text as id,
            contrato_id::text as contrato_id,
            unidade_gestora,
            gestao,
            numero as nota_empenho,
            credor,
            fonte_recurso,
            programa_trabalho,
            planointerno,
            naturezadespesa,
            informacao_complementar,
            sistema_origem,
            links__documento_pagamento as links_documento_pagamento,
            credor_obj__tipo as credor_obj_tipo,
            credor_obj__cnpj_cpf_idgener as credor_obj_cnpj_cpf_idgener,
            credor_obj__nome as credor_obj_nome,

            -- Tratar valores nulos ou inválidos nas colunas de data
            replace(replace(empenhado::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as empenhado,

            -- Conversão de valores numéricos para FLOAT
            replace(replace(aliquidar::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as aliquidar,
            replace(replace(liquidado::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as liquidado,
            replace(replace(pago::text, '.', ''), ',', '.')::numeric(15, 2) as pago,
            replace(replace(rpinscrito::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as rpinscrito,
            replace(replace(rpaliquidar::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as rpaliquidar,
            replace(replace(rpliquidado::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as rpliquidado,
            replace(replace(rppago::text, '.', ''), ',', '.')::numeric(15, 2) as rppago,
            case
                when
                    data_emissao is not null
                    and data_emissao::text ~ '^\d{4}-\d{2}-\d{2}$'
                -- Retorna NULL se não for uma data válida
                then to_date(data_emissao::text, 'YYYY-MM-DD')
            end as data_emissao

        from {{ source("compras_gov", "empenhos") }}
    )

select *
from empenhos
