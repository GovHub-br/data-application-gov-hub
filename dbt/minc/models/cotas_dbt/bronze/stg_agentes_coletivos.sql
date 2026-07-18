-- Perfil LPG Coletivos/Grupos: base + variantes. Demografia = maioria do grupo.
with unioned as (
    select
        "nº do cpf do representante do grupo/coletivo"        as documento,
        "raça, cor ou etnia da maioria do grupo/coletivo"     as raca_bruto,
        "há pessoa com deficiência – pcd no grupo/coletivo?"  as pcd_bruto,
        cep, cidade, uf,
        'lpg_dados_coletivos'                                 as origem_tabela,
        nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_coletivos') }}
    union all
    select
        "nº do cpf do representante do grupo/coletivo",
        "raça, cor ou etnia da maioria do grupo/coletivo",
        "há pessoa com deficiência – pcd no grupo/coletivo?",
        cep, cidade, uf, 'lpg_dados_grupo_coletivo', nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_grupo_coletivo') }}
    union all
    select
        "nº do cpf do representante do grupo/coletivo",
        "raça, cor ou etnia da maioria do grupo/coletivo",
        "há pessoa com deficiência – pcd no grupo/coletivo?",
        cep, cidade, uf, 'lpg_dados_grupo_coletivo_audiovisua', nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_grupo_coletivo_audiovisua') }}
    union all
    select
        "nº do cpf do representante do grupo/coletivo",
        "raça, cor ou etnia da maioria do grupo/coletivo",
        "há pessoa com deficiência – pcd no grupo/coletivo?",
        cep, cidade, uf, 'lpg_dados_grupo_coletivo_multicultu', nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_grupo_coletivo_multicultu') }}
)
select
    {{ normaliza_documento('documento') }} as identificador_unico,
    'coletivo'                              as tipo_proponente,
    'lpg'                                   as origem,
    raca_bruto,
    pcd_bruto,
    cast(null as text)                      as indigena_bruto,
    cast(null as text)                      as quilombola_bruto,
    cep, cidade, uf, origem_tabela, nome_programa
from unioned
where {{ normaliza_documento('documento') }} is not null
