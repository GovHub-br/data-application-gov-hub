{{ config(materialized="table") }}

with
    faturas_raw as (
        select
            id::integer as id,
            contrato_id::integer as contrato_id,
            tipolistafatura_id::text as tipolistafatura_id,
            justificativafatura_id::text as justificativafatura_id,
            sfadrao_id::text as sfadrao_id,
            numero::text as numero,
            emissao::date as emissao,
            prazo::date as prazo,
            vencimento::date as vencimento,
            -- Limpar o formato numérico das colunas que têm problemas
            replace(replace(valor::text, '.', ''), ',', '.')::numeric(15, 2) as valor,
            replace(replace(juros::text, '.', ''), ',', '.')::numeric(15, 2) as juros,
            replace(replace(multa::text, '.', ''), ',', '.')::numeric(15, 2) as multa,
            replace(replace(glosa::text, '.', ''), ',', '.')::numeric(15, 2) as glosa,
            replace(replace(valorliquido::text, '.', ''), ',', '.')::numeric(
                15, 2
            ) as valorliquido,
            processo::text as processo,
            protocolo::date as protocolo,
            ateste::date as ateste,
            repactuacao::text as repactuacao,
            infcomplementar::text as infcomplementar,
            mesref::integer as mesref,
            anoref::integer as anoref,
            situacao::text as situacao,
            chave_nfe::text as chave_nfe,
            dados_referencia::text as dados_referencia,
            dados_item_faturado::text as dados_item_faturado,
            jsonb_array_elements(
                replace(dados_empenho, '''', '"')::jsonb
            ) as dados_empenho
        from {{ source("compras_gov", "faturas") }}
    ),
    -- Extrai os campos do JSON e transforma em colunas individuais
    faturas_dados_empenho as (
        select
            f.*,
            f.dados_empenho ->> 'id_empenho' as id_empenho,
            upper(f.dados_empenho ->> 'numero_empenho') as numero_empenho,
            f.dados_empenho ->> 'valor_empenho' as valor_empenho,
            f.dados_empenho ->> 'subelemento' as subelemento
        from faturas_raw as f
    )  --
select *
from faturas_dados_empenho
