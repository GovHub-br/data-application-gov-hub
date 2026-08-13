-- Editais (denominador / derivação de ano). lpg_editais + instrumentos (x4).
-- Colunas via coalesce_por_nome (lpg_editais tem "valor total do edital" duplicada).
-- lpg_editais virou planilha_editais_lpg; as quatro abas de instrumentos são
-- fatias de planilha_dados_lpg, separadas por tabela_origem.
{% set ed  = source('relatorio_gestao', 'planilha_editais_lpg') %}
{% set inst = source('relatorio_gestao', 'planilha_dados_lpg') %}
with editais as (
    select
        {{ coalesce_por_nome(ed, ['nome do edital']) }}        as nome_edital,
        cast(null as text)                                     as numero_edital,
        {{ coalesce_por_nome(ed, ['valor total do edital']) }} as valor_total_raw,
        nome_arquivo, nome_programa
    from {{ ed }}
    union all
    -- As quatro abas de instrumentos eram quatro tabelas com o mesmo layout;
    -- hoje são um filtro só.
    select
        {{ coalesce_por_nome(inst, ['título do edital']) }}        as nome_edital,
        {{ coalesce_por_nome(inst, ['número do edital']) }}        as numero_edital,
        {{ coalesce_por_nome(inst, ['valor total do edital']) }}   as valor_total_raw,
        nome_arquivo,
        nome_programa
    from {{ inst }}
    where tabela_origem in (
        'lpg_dados_instrumentos',
        'lpg_dados_instrumentos_2',
        'lpg_dados_instrumentos_2_2',
        'lpg_dados_instrumentos_publicos'
    )
)
select
    nome_edital,
    numero_edital,
    {{ parse_valor('valor_total_raw') }} as valor_total,
    nome_arquivo,
    nome_programa
from editais
where nome_edital is not null or numero_edital is not null
