-- Editais (denominador / derivação de ano). lpg_editais + instrumentos (x4).
-- Colunas via coalesce_por_nome (lpg_editais tem "valor total do edital" duplicada).
{% set ed   = source('transferegov_fundo_a_fundo', 'lpg_editais') %}
{% set i1   = source('transferegov_fundo_a_fundo', 'lpg_dados_instrumentos') %}
{% set i2   = source('transferegov_fundo_a_fundo', 'lpg_dados_instrumentos_2') %}
{% set i22  = source('transferegov_fundo_a_fundo', 'lpg_dados_instrumentos_2_2') %}
{% set ipub = source('transferegov_fundo_a_fundo', 'lpg_dados_instrumentos_publicos') %}
with editais as (
    select
        {{ coalesce_por_nome(ed, ['nome do edital']) }}        as nome_edital,
        cast(null as text)                                     as numero_edital,
        {{ coalesce_por_nome(ed, ['valor total do edital']) }} as valor_total_raw,
        nome_arquivo, nome_programa
    from {{ ed }}
    union all
    select {{ coalesce_por_nome(i1, ['título do edital']) }}, {{ coalesce_por_nome(i1, ['número do edital']) }}, {{ coalesce_por_nome(i1, ['valor total do edital']) }}, nome_arquivo, nome_programa
    from {{ i1 }}
    union all
    select {{ coalesce_por_nome(i2, ['título do edital']) }}, {{ coalesce_por_nome(i2, ['número do edital']) }}, {{ coalesce_por_nome(i2, ['valor total do edital']) }}, nome_arquivo, nome_programa
    from {{ i2 }}
    union all
    select {{ coalesce_por_nome(i22, ['título do edital']) }}, {{ coalesce_por_nome(i22, ['número do edital']) }}, {{ coalesce_por_nome(i22, ['valor total do edital']) }}, nome_arquivo, nome_programa
    from {{ i22 }}
    union all
    select {{ coalesce_por_nome(ipub, ['título do edital']) }}, {{ coalesce_por_nome(ipub, ['número do edital']) }}, {{ coalesce_por_nome(ipub, ['valor total do edital']) }}, nome_arquivo, nome_programa
    from {{ ipub }}
)
select
    nome_edital,
    numero_edital,
    {{ parse_valor('valor_total_raw') }} as valor_total,
    nome_arquivo,
    nome_programa
from editais
where nome_edital is not null or numero_edital is not null
