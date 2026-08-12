-- Perfil LPG Pessoa Física: base + variantes audiovisual/multicultural.
-- Filtra o lixo de parsing (linhas sem CPF). Chave normalizada.
-- As três variantes eram tabelas separadas; hoje são fatias de
-- planilha_dados_lpg identificadas por tabela_origem — o union all virou filtro.
with unioned as (
    select
        "nº do cpf"                      as documento,
        "raça, cor ou etnia"             as raca_bruto,
        "é pessoa com deficiência –pcd?" as pcd_bruto,
        cep,
        cidade,
        uf,
        tabela_origem                    as origem_tabela,
        nome_programa
    from {{ source('relatorio_gestao', 'planilha_dados_lpg') }}
    where tabela_origem in (
        'lpg_dados_pessoa_fisica',
        'lpg_dados_pessoa_fisica_audiovisual',
        'lpg_dados_pessoa_fisica_multicultur'
    )
)
select
    {{ normaliza_documento('documento') }} as identificador_unico,
    'pf'                                    as tipo_proponente,
    'lpg'                                   as origem,
    raca_bruto,
    pcd_bruto,
    cast(null as text)                      as indigena_bruto,
    cast(null as text)                      as quilombola_bruto,
    cep,
    cidade,
    uf,
    origem_tabela,
    nome_programa
from unioned
where {{ normaliza_documento('documento') }} is not null
