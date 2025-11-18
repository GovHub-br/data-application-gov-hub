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
            du.nome_municipio_uorg
        from {{ ref("dados_funcionais") }} df
        inner join {{ ref("dados_uorg") }} du on df.sigla_uorg_exercicio = du.sigla_uorg
    )

select
    case
        when nome_situacao_funcional = 'APOSENTADO'
        then 'Aposentado'
        when nome_situacao_funcional = 'INSTITUIDOR PENSAO'
        then 'Pensionista'
        when nome_situacao_funcional = 'ESTAGIARIO SIGEPE'
        then 'Estagiário'
        when nome_situacao_funcional = 'NOMEADO CARGO COMIS.'
        then 'Cargo comissionado'
        when nome_situacao_funcional = 'ATIVO EM OUTRO ORGAO'
        then 'Cedido'
        when nome_situacao_funcional = 'EXERC DESCENT CARREI'
        then 'Requisitado'
        when nome_situacao_funcional = 'ATIVO PERMANENTE'
        then 'Ativo permanente'
        when nome_situacao_funcional = 'CEDIDO/REQUISITADO'
        then 'Requisitado'
        when
            regexp_replace(nome_situacao_funcional, '[\s]+', ' ', 'g')
            ilike 'EXERC. 7 ART93 8112'
        then 'Requisitado'
    end as situacao_funcional,
    nome_situacao_funcional as situacao_funcional_original,
    count(nome_situacao_funcional) as quantidade_servidores
from dados_funcionais_enriquecidos
group by
    case
        when nome_situacao_funcional = 'APOSENTADO'
        then 'Aposentado'
        when nome_situacao_funcional = 'INSTITUIDOR PENSAO'
        then 'Pensionista'
        when nome_situacao_funcional = 'ESTAGIARIO SIGEPE'
        then 'Estagiário'
        when nome_situacao_funcional = 'NOMEADO CARGO COMIS.'
        then 'Cargo comissionado'
        when nome_situacao_funcional = 'ATIVO EM OUTRO ORGAO'
        then 'Cedido'
        when nome_situacao_funcional = 'EXERC DESCENT CARREI'
        then 'Requisitado'
        when nome_situacao_funcional = 'ATIVO PERMANENTE'
        then 'Ativo permanente'
        when nome_situacao_funcional = 'CEDIDO/REQUISITADO'
        then 'Requisitado'
        when
            regexp_replace(nome_situacao_funcional, '[\s]+', ' ', 'g')
            ilike 'EXERC. 7 ART93 8112'
        then 'Requisitado'
    end,
    nome_situacao_funcional
order by quantidade_servidores desc




select
  distinct 
  df.*,
  CASE
    WHEN df.modalidade_pgd is null THEN 'Não participa'
    WHEN df.modalidade_pgd='parcial' THEN 'Parcial'
    WHEN df.modalidade_pgd='integral' THEN 'Integral'
    WHEN df.modalidade_pgd='presencial' THEN 'Presencial'
    WHEN df.modalidade_pgd='no exterior' THEN 'No exterior'
  END as pdg,
  CASE
    WHEN df.nome_situacao_funcional='ATIVO EM OUTRO ORGAO' THEN 'Ativo em outro órgão'
    ELSE df.sigla_uorg_exercicio
  END as unidade_exercicio,
  du.nome_municipio_uorg 
from pessoas.dados_funcionais df
inner join pessoas.dados_uorg du
on df.sigla_uorg_exercicio=du.sigla_uorg