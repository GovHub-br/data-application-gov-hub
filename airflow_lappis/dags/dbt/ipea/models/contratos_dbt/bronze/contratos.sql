{{ config(materialized="table") }}

with
    contratos_raw as (
        select
            -- Conversão de tipos e formatação de colunas
            cast(id as text) as id,
            receita_despesa,
            numero,
            contratante__orgao_origem__codigo,
            contratante__orgao_origem__nome,
            contratante__orgao_origem__unidade_gestora_origem__codigo,
            contratante__orgao_origem__unidade_gestora_origem__nome_resumido,
            contratante__orgao_origem__unidade_gestora_origem__nome,
            contratante__orgao_origem__unidade_gestora_origem__sisg,
            contratante__orgao_origem__unidade_gestora_origem__utiliza_siafi,
            contratante__orgao_origem__unidade_gestora_origem__utiliza_antecip,
            contratante__orgao__codigo,
            contratante__orgao__nome,
            contratante__orgao__unidade_gestora__codigo,
            contratante__orgao__unidade_gestora__nome_resumido,
            contratante__orgao__unidade_gestora__nome,
            contratante__orgao__unidade_gestora__sisg,
            contratante__orgao__unidade_gestora__utiliza_siafi,
            contratante__orgao__unidade_gestora__utiliza_antecipagov,
            fornecedor__tipo as fornecedor_tipo,
            fornecedor__nome as fornecedor_nome,
            codigo_tipo as codigo_tipo,
            tipo,
            subtipo,
            prorrogavel,
            situacao,
            justificativa_inativo,
            categoria,
            subcategoria,
            unidades_requisitantes,
            objeto,
            amparo_legal,
            informacao_complementar,
            codigo_modalidade,
            modalidade,
            unidade_compra as unidade_compra,
            licitacao_numero,
            sistema_origem_licitacao,
            cast(num_parcelas as int) as num_parcelas,
            cast(
                replace(
                    replace(cast(valor_inicial as text), '.', ''), ',', '.'
                ) as numeric(15, 2)
            ) as valor_inicial,
            -- Tratar valores nulos ou inválidos nas colunas de data
            cast(
                replace(
                    replace(cast(valor_global as text), '.', ''), ',', '.'
                ) as numeric(15, 2)
            ) as valor_global,
            cast(
                replace(
                    replace(cast(valor_parcela as text), '.', ''), ',', '.'
                ) as numeric(15, 2)
            ) as valor_parcela,
            cast(
                replace(
                    replace(cast(valor_acumulado as text), '.', ''), ',', '.'
                ) as numeric(15, 2)
            ) as valor_acumulado,
            regexp_replace(
                fornecedor__cnpj_cpf_idgener, '[^0-9A-Za-z]', '', 'g'
            ) as fornecedor_cnpj_cpf_idgener,
            regexp_replace(processo, '[^0-9A-Za-z]', '', 'g') as processo,
            -- Conversão de valores numéricos para FLOAT ou INT
            case
                when data_assinatura is null
                then null
                when
                    data_assinatura is not null
                    and cast(data_assinatura as text) ~ '^\d{4}-\d{2}-\d{2}$'
                -- Retorna NULL se não for uma data válida
                then to_date(cast(data_assinatura as text), 'YYYY-MM-DD')
            end as data_assinatura,
            case
                when data_publicacao is null
                then null
                when
                    data_publicacao is not null
                    and cast(data_publicacao as text) ~ '^\d{4}-\d{2}-\d{2}$'
                -- Retorna NULL se não for uma data válida
                then to_date(cast(data_publicacao as text), 'YYYY-MM-DD')
            end as data_publicacao,
            case
                when data_proposta_comercial is null
                then null
                when
                    data_proposta_comercial is not null
                    and cast(data_proposta_comercial as text) ~ '^\d{4}-\d{2}-\d{2}$'
                then
                    -- Retorna NULL se não for uma data válida
                    to_date(cast(data_proposta_comercial as text), 'YYYY-MM-DD')
            end as data_proposta_comercial,
            case
                when vigencia_inicio is null
                then null
                when
                    vigencia_inicio is not null
                    and cast(vigencia_inicio as text) ~ '^\d{4}-\d{2}-\d{2}$'
                -- Retorna NULL se não for uma data válida
                then to_date(cast(vigencia_inicio as text), 'YYYY-MM-DD')
            end as vigencia_inicio,
            case
                when vigencia_fim is null
                then null
                when
                    vigencia_fim is not null
                    and cast(vigencia_fim as text) ~ '^\d{4}-\d{2}-\d{2}$'
                -- Retorna NULL se não for uma data válida
                then to_date(cast(vigencia_fim as text), 'YYYY-MM-DD')
            end as vigencia_fim
        from {{ source("compras_gov", "contratos") }}
    )  --

select *
from contratos_raw
