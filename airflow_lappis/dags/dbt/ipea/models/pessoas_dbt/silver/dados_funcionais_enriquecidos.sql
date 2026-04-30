select distinct
    df.*,
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
        when df.nome_situacao_funcional = 'ATIVO EM OUTRO ORGAO'
        then 'Ativo em outro órgão'
        else df.sigla_uorg_exercicio
    end as unidade_exercicio,
    du.nome_municipio_uorg
from {{ ref("dados_funcionais") }} as df
inner join {{ ref("dados_uorg") }} as du on df.sigla_uorg_exercicio = du.sigla_uorg
