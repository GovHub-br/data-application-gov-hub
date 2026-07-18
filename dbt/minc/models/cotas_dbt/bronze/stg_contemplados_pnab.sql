-- Contemplados PNAB (lado-valor): geral + pncv. Colunas via coalesce_por_nome.
-- Chave = CNPJ real (casável) ou CPF anonimizado (flag, não casável).
-- anexo_id: liga ao ano-do-edital-por-anexo (abas de definição do mesmo arquivo).
{% set geral = source('transferegov_fundo_a_fundo', 'raw_pnab_lista_contemplados_geral') %}
{% set pncv = source('transferegov_fundo_a_fundo', 'raw_pnab_lista_contemplados_pncv') %}
with base as (
    select
        id_anexo,
        {{ coalesce_por_nome(geral, ['cpf (anonimizado)']) }}                                as cpf_anon,
        {{ coalesce_por_nome(geral, ['cnpj']) }}                                             as cnpj_raw,
        {{ coalesce_por_nome(geral, ['valor pago ao contemplado ou contratado', 'valor pago']) }} as valor_raw,
        {{ coalesce_por_nome(geral, ['nome da atividade', 'nome do edital']) }}              as nome_edital,
        nome_arquivo,
        nome_programa
    from {{ geral }}
    union all
    select
        id_anexo,
        {{ coalesce_por_nome(pncv, ['cpf (anonimizado)']) }},
        {{ coalesce_por_nome(pncv, ['cnpj']) }},
        {{ coalesce_por_nome(pncv, ['valor pago ao contemplado', 'valor pago']) }},
        {{ coalesce_por_nome(pncv, ['número e título do edital']) }},
        nome_arquivo,
        nome_programa
    from {{ pncv }}
),
norm as (
    select
        substring(id_anexo from 'anexo_([0-9]+)') as anexo_id,
        {{ normaliza_documento('cnpj_raw') }} as cnpj_norm,
        {{ normaliza_documento('cpf_anon') }} as cpf_norm,
        cpf_anon, valor_raw, nome_edital, nome_arquivo, nome_programa
    from base
),
final as (
    select
        anexo_id,
        -- CPF anonimizado (com '*') não é chave casável -> só CNPJ real ou CPF real viram chave
        coalesce(cnpj_norm, case when cpf_anon like '%*%' then null else cpf_norm end) as identificador_unico,
        (cnpj_norm is null and cpf_anon like '%*%') as chave_anonimizada,
        {{ parse_valor('valor_raw') }}              as valor_pago_num,
        {{ parse_valor('valor_raw', teto=none) }}   as valor_bruto_num,
        nome_edital,
        cast(null as text)                          as link,
        nome_arquivo,
        nome_programa,
        'pnab'                                      as origem
    from norm
)
select *
from final
where identificador_unico is not null or valor_pago_num is not null
