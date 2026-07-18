-- Perfil PNAB Organizações (pnab_organizacoes): CNPJ real + raça/PCD do representante.
with base as (
    select
        "nº do cnpj"                                                as documento,
        "raça, cor ou etnia do representante legal da organização"  as raca_bruto,
        "é pessoa com deficiência –pcd?"                            as pcd_bruto,
        "indígena?"                                                 as indigena_bruto,
        "quilombola?"                                               as quilombola_bruto,
        cep,
        "cidade/uf"                                                 as cidade,
        nome_programa
    from {{ source('transferegov_fundo_a_fundo', 'pnab_organizacoes') }}
)
select
    {{ normaliza_documento('documento') }} as identificador_unico,
    'pj'                                    as tipo_proponente,
    'pnab'                                  as origem,
    raca_bruto,
    pcd_bruto,
    indigena_bruto,
    quilombola_bruto,
    cep,
    cidade,
    cast(null as text)                      as uf,
    'pnab_organizacoes'                     as origem_tabela,
    nome_programa
from base
where {{ normaliza_documento('documento') }} is not null
