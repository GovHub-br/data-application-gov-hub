-- Modelo para gerar a distribuição geográfica de servidores por UF
-- Retorna todos os estados brasileiros com suas respectivas contagens e percentuais
with
    -- Obter todos os servidores com localização
    servidores_localizacao as (
        select distinct
            df.cpf, du.uf_uorg, du.nome_municipio_uorg, df.nome_situacao_funcional,
            greatest(df.dt_ingest, du.dt_ingest) as dt_ingest
        from {{ ref("dados_funcionais") }} df
        inner join {{ ref("dados_uorg") }} du on df.sigla_uorg_exercicio = du.sigla_uorg
        where du.uf_uorg is not null
    ),

    -- Contar servidores por UF
    contagem_por_uf as (
        select uf_uorg, count(distinct cpf) as valor, max(dt_ingest) as dt_ingest_uf
        from servidores_localizacao
        group by uf_uorg
    ),

    -- Calcular totais para percentual
    total_servidores as (select sum(valor) as total from contagem_por_uf)

-- Juntar todos os estados com suas contagens (0 para estados sem servidores)
select
    eb.sigla_uf,
    eb.nome_uf,
    coalesce(cpu.valor, 0) as valor,
    case
        when coalesce(cpu.valor, 0) = 0
        then '0%'
        else concat(round((coalesce(cpu.valor, 0) * 100.0 / ts.total), 0), '%')
    end as percentual,
    cpu.dt_ingest_uf as dt_ingest
from {{ ref("estados_brasil") }} eb
cross join total_servidores ts
left join contagem_por_uf cpu on eb.sigla_uf = cpu.uf_uorg
order by eb.sigla_uf
