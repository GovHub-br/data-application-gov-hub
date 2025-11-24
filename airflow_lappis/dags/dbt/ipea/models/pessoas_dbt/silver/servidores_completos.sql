-- Modelo intermediário que centraliza os enriquecimentos de dados de servidores
-- Combina informações de hierarquia, dados funcionais, organizacionais e pessoais
-- Este modelo evita duplicação de código nos modelos gold
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
    )

select distinct
    se.*,
    sd.cod_escolaridade_principal,
    sd.nome_escolaridade_principal,
    sd.nome_deficiencia_fisica,
    sd.nome_cargo as nome_cargo_emprego
from servidores_enriquecidos se
inner join {{ ref("servidores_detalhados") }} sd on se.cpf = sd.cpf
