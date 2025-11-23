-- Modelo para gerar a distribuição geográfica de servidores por UF
-- Retorna todos os estados brasileiros com suas respectivas contagens e percentuais
with
    -- Obter todos os servidores com localização
    servidores_localizacao as (
        select distinct
            df.cpf, du.uf_uorg, du.nome_municipio_uorg, df.nome_situacao_funcional
        from {{ ref("dados_funcionais") }} df
        inner join {{ ref("dados_uorg") }} du on df.sigla_uorg_exercicio = du.sigla_uorg
        where du.uf_uorg is not null
    ),

    -- Contar servidores por UF
    contagem_por_uf as (
        select uf_uorg, count(distinct cpf) as valor
        from servidores_localizacao
        group by uf_uorg
    ),

    -- Calcular totais para percentual
    total_servidores as (select sum(valor) as total from contagem_por_uf),

    -- Lista de todos os estados brasileiros com nomes completos
    estados_brasil as (
        select 'AC' as sigla_uf, 'ACRE' as nome_uf
        union all
        select 'AL', 'ALAGOAS'
        union all
        select 'AP', 'AMAPÁ'
        union all
        select 'AM', 'AMAZONAS'
        union all
        select 'BA', 'BAHIA'
        union all
        select 'CE', 'CEARÁ'
        union all
        select 'DF', 'DISTRITO FEDERAL'
        union all
        select 'ES', 'ESPÍRITO SANTO'
        union all
        select 'GO', 'GOIÁS'
        union all
        select 'MA', 'MARANHÃO'
        union all
        select 'MT', 'MATO GROSSO'
        union all
        select 'MS', 'MATO GROSSO DO SUL'
        union all
        select 'MG', 'MINAS GERAIS'
        union all
        select 'PA', 'PARÁ'
        union all
        select 'PB', 'PARAÍBA'
        union all
        select 'PR', 'PARANÁ'
        union all
        select 'PE', 'PERNAMBUCO'
        union all
        select 'PI', 'PIAUÍ'
        union all
        select 'RJ', 'RIO DE JANEIRO'
        union all
        select 'RN', 'RIO GRANDE DO NORTE'
        union all
        select 'RS', 'RIO GRANDE DO SUL'
        union all
        select 'RO', 'RONDÔNIA'
        union all
        select 'RR', 'RORAIMA'
        union all
        select 'SC', 'SANTA CATARINA'
        union all
        select 'SP', 'SÃO PAULO'
        union all
        select 'SE', 'SERGIPE'
        union all
        select 'TO', 'TOCANTINS'
    )

-- Juntar todos os estados com suas contagens (0 para estados sem servidores)
select
    eb.sigla_uf,
    eb.nome_uf,
    coalesce(cpu.valor, 0) as valor,
    case
        when coalesce(cpu.valor, 0) = 0
        then '0%'
        else concat(round((coalesce(cpu.valor, 0) * 100.0 / ts.total), 0), '%')
    end as percentual
from estados_brasil eb
cross join total_servidores ts
left join contagem_por_uf cpu on eb.sigla_uf = cpu.uf_uorg
order by eb.sigla_uf
