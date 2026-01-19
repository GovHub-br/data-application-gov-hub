with
    dados_funcionais_enriquecidos as (
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
            du.nome_municipio_uorg,
            greatest(df.dt_ingest, du.dt_ingest) as dt_ingest_max
        from {{ ref("dados_funcionais") }} df
        inner join {{ ref("dados_uorg") }} du on df.sigla_uorg_exercicio = du.sigla_uorg
    )

select
    nome_situacao_funcional as situacao_funcional_original,
    count(nome_situacao_funcional) as quantidade_servidores,
    max(dt_ingest_max) as dt_ingest
from dados_funcionais_enriquecidos
group by nome_situacao_funcional
order by quantidade_servidores desc
