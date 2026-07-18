-- Perfil LPG Pessoa Física: base + variantes audiovisual/multicultural.
-- Filtra o lixo de parsing (linhas sem CPF). Chave normalizada.
with unioned as (
    select
        "nº do cpf"                         as documento,
        "raça, cor ou etnia"                as raca_bruto,
        "é pessoa com deficiência –pcd?"    as pcd_bruto,
        cep, cidade, uf,
        'lpg_dados_pessoa_fisica'           as origem_tabela,
        nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_fisica') }}
    union all
    select
        "nº do cpf", "raça, cor ou etnia", "é pessoa com deficiência –pcd?",
        cep, cidade, uf, 'lpg_dados_pessoa_fisica_audiovisual', nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_fisica_audiovisual') }}
    union all
    select
        "nº do cpf", "raça, cor ou etnia", "é pessoa com deficiência –pcd?",
        cep, cidade, uf, 'lpg_dados_pessoa_fisica_multicultur', nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'lpg_dados_pessoa_fisica_multicultur') }}
)
select
    {{ normaliza_documento('documento') }} as identificador_unico,
    'pf'                                    as tipo_proponente,
    'lpg'                                   as origem,
    raca_bruto,
    pcd_bruto,
    cast(null as text)                      as indigena_bruto,
    cast(null as text)                      as quilombola_bruto,
    cep, cidade, uf, origem_tabela, nome_programa
from unioned
where {{ normaliza_documento('documento') }} is not null
