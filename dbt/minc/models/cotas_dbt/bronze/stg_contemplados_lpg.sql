-- Contemplados LPG (lado-valor). Colunas duplicadas por schema-drift resolvidas
-- via coalesce_por_nome (lê nomes reais, imune a espaço/trailing).
{% set src = source('relatorio_gestao', 'planilha_contemplados_lpg') %}
with base as (
    select
        id_anexo                                                                    as anexo_id,
        {{ coalesce_por_nome(src, ['cpf ou cnpj', 'cnpj']) }}                        as doc_raw,
        {{ coalesce_por_nome(src, ['valor pago']) }}                                as valor_raw,
        {{ coalesce_por_nome(src, ['nome do edital']) }}                            as nome_edital,
        {{ coalesce_por_nome(src, ['link da publicação do resultado do edital']) }} as link,
        nome_arquivo,
        nome_programa
    from {{ src }}
),
final as (
    select
        anexo_id,
        -- doc mascarado (com '*') não é chave casável -> NULL (mas mantém o valor)
        case when {{ documento_anonimizado('doc_raw') }} then null
             else {{ normaliza_documento('doc_raw') }} end as identificador_unico,
        {{ documento_anonimizado('doc_raw') }} as chave_anonimizada,
        {{ parse_valor('valor_raw') }}              as valor_pago_num,
        {{ parse_valor('valor_raw', teto=none) }}   as valor_bruto_num,
        nome_edital,
        link,
        nome_arquivo,
        nome_programa,
        'lpg'                                  as origem
    from base
)
select *
from final
where identificador_unico is not null or valor_pago_num is not null
