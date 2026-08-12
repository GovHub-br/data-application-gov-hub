-- Perfil LPG Pessoa Jurídica: base + variantes. Demografia = corpo diretivo.
-- As variantes eram tabelas separadas; hoje são fatias de planilha_dados_lpg
-- identificadas por tabela_origem.
with unioned as (
    select
        "nº do cnpj"                                               as documento,
        "raça, cor ou etnia da maioria do corpo diretivo da pj"    as raca_bruto,
        "há pessoa com deficiência – pcd no corpo diretivo da pj?" as pcd_bruto,
        cep,
        cidade,
        uf,
        tabela_origem                                              as origem_tabela,
        nome_programa
    from {{ source('relatorio_gestao', 'planilha_dados_lpg') }}
    where tabela_origem in (
        'lpg_dados_pessoa_juridica',
        'lpg_dados_pessoa_juridica_audiovisu',
        'lpg_dados_pessoa_juridica_multicult'
    )
)
select
    {{ normaliza_documento('documento') }} as identificador_unico,
    'pj'                                    as tipo_proponente,
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
