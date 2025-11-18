with
    hierarquia_enriquecida as (
        select
            ph.*,
            case
                when df.modalidade_pgd is null
                then 'Não participa'
                when df.modalidade_pgd = 'parcial'
                then 'Parcial'
                when df.modalidade_pgd = 'integral'
                then 'Integral'
                when df.modalidade_pgd = 'presencial'
                then 'Presencial'
                when df.modalidade_pgd = 'no exterior'
                then 'No exterior'
            end as pdg,
            case
                when ph.nome_situacao_funcional = 'ATIVO EM OUTRO ORGAO'
                then 'Ativo em outro órgão'
                else siglaunidade
            end as unidade_exercicio
        from {{ ref("hierarquia") }} ph
        inner join {{ ref("dados_funcionais") }} df on ph.cpf = df.cpf
    ),

    servidores_enriquecidos as (
        select distinct ph.*, du.nome_municipio_uorg
        from hierarquia_enriquecida ph
        inner join {{ ref("dados_uorg") }} du on ph.siglaunidade = du.sigla_uorg
        order by caminho_unidade, hierarquia_cargo
    ),

    servidores_completos as (
        select distinct
            se.*,
            sd.cod_escolaridade_principal,
            sd.nome_escolaridade_principal,
            sd.nome_deficiencia_fisica,
            sd.nome_cargo as nome_cargo_emprego
        from servidores_enriquecidos se
        inner join {{ ref("servidores_detalhados") }} sd on se.cpf = sd.cpf
    )

select
    nome_sexo as genero,
    count(*) as quantidade_servidores,
    count(*) * 1.0 / sum(count(*)) over () as percentual_distribuicao
from servidores_completos
group by nome_sexo
order by percentual_distribuicao desc
