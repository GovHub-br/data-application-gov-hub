-- Modelo para gerar tabela de servidores com agregação por cargo, gênero, situação e
-- localização
-- Agrupa os dados de servidores para visualização em tabelas detalhadas
with
    servidores_completos as (
        select
            df.cpf,
            df.nome_cargo,
            dp.nome_sexo as genero,
            df.nome_situacao_funcional as situacao,
            du.nome_municipio_uorg as cidade,
            du.uf_uorg as estado
        from {{ ref("dados_funcionais") }} df
        inner join {{ ref("dados_pessoais") }} dp on df.cpf = dp.cpf
        inner join {{ ref("dados_uorg") }} du on df.sigla_uorg_exercicio = du.sigla_uorg
        where
            df.nome_cargo is not null
            and dp.nome_sexo is not null
            and df.nome_situacao_funcional is not null
            and du.nome_municipio_uorg is not null
            and du.uf_uorg is not null
    ),

    servidores_agregados as (
        select
            nome_cargo as cargo,
            case
                when upper(genero) = 'MASCULINO'
                then 'Masculino'
                when upper(genero) = 'FEMININO'
                then 'Feminino'
                else genero
            end as genero,
            case
                when upper(situacao) = 'ATIVO PERMANENTE'
                then 'Ativo Permanente'
                when upper(situacao) = 'APOSENTADO'
                then 'Aposentado'
                when upper(situacao) = 'ATIVO EM OUTRO ORGAO'
                then 'Ativo em outro órgão'
                when upper(situacao) = 'ESTAGIARIO SIGEPE'
                then 'Estagiário'
                when
                    upper(situacao) like '%CEDIDO%'
                    or upper(situacao) like '%REQUISITADO%'
                then 'Cedido/Requisitado'
                else situacao
            end as situacao,
            initcap(cidade) as cidade,
            upper(estado) as estado,
            count(distinct cpf) as total
        from servidores_completos
        group by nome_cargo, genero, situacao, cidade, estado
    )

select cargo, genero, situacao, cidade, estado, total
from servidores_agregados
where total > 0
order by total desc, cargo, genero
