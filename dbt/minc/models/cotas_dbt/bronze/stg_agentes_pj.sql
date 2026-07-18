-- Perfil LPG Pessoa Jurídica: base + variantes. Demografia = corpo diretivo.
with unioned as (
    select
        "nº do cnpj"                                              as documento,
        "raça, cor ou etnia da maioria do corpo diretivo da pj"   as raca_bruto,
        "há pessoa com deficiência – pcd no corpo diretivo da pj?" as pcd_bruto,
        cep, cidade, uf,
        'lpg_dados_pessoa_juridica'                               as origem_tabela,
        nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_juridica') }}
    union all
    select
        "nº do cnpj", "raça, cor ou etnia da maioria do corpo diretivo da pj",
        "há pessoa com deficiência – pcd no corpo diretivo da pj?",
        cep, cidade, uf, 'lpg_dados_pessoa_juridica_audiovisu', nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_juridica_audiovisu') }}
    union all
    select
        "nº do cnpj", "raça, cor ou etnia da maioria do corpo diretivo da pj",
        "há pessoa com deficiência – pcd no corpo diretivo da pj?",
        cep, cidade, uf, 'lpg_dados_pessoa_juridica_multicult', nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_juridica_multicult') }}
)
select
    {{ normaliza_documento('documento') }} as identificador_unico,
    'pj'                                    as tipo_proponente,
    'lpg'                                   as origem,
    raca_bruto,
    pcd_bruto,
    cast(null as text)                      as indigena_bruto,
    cast(null as text)                      as quilombola_bruto,
    cep, cidade, uf, origem_tabela, nome_programa
from unioned
where {{ normaliza_documento('documento') }} is not null
