with

    correcao_funcao as (
        select *, replace(funcao, ' ', '') as funcao_sigla
        from {{ ref("estrutura_organizacional_cargos") }}
    ),

    codigos_siorg as (
        select distinct
            funcao_sigla,
            eorg.nomeunidade,
            eorg.codigounidade,
            eorg.ordem_grandeza,
            eorg.denominacao,
            eorg.codigo_instancia,
            uo.codigounidadepai,
            uo.caminho_unidade,
            case
                when eorg.siglaunidade = 'GABIN-IPEA' then 'GABIN' else siglaunidade
            end as siglaunidade,
            substring(funcao_sigla, length(funcao_sigla) - 2, 1) as categoria_cargo,
            -- hierarquia do cargo está sendo definida a partir da fórmula:
            -- (categoria do cargo * 1000) - nível do cargo
            -- quanto menor a hierarquia, maior o cargo
            right(funcao_sigla, 2) as nivel_cargo,
            cast(substring(funcao_sigla, length(funcao_sigla) - 2, 1) as int) * 1000
            - cast(right(funcao, 2) as int) as hierarquia_cargo
        from correcao_funcao as eorg
        inner join
            {{ ref("unidade_organizacional") }} as uo
            on eorg.codigounidade = uo.codigounidade
    ),

    codigos_siape as (
        select distinct
            df.cod_funcao,
            df.nome_uorg_exercicio,
            df.sigla_uorg_exercicio,
            df.nome_cargo,
            df.matricula_siape,
            df.cpf,
            df.cpf_chefia_imediata,
            df.cod_situacao_funcional,
            df.nome_situacao_funcional,
            dp.nome_pessoa,
            dp.dt_nascimento,
            dp.nome_sexo,
            dp.nome_estado_civil,
            dp.nome_nacionalidade,
            dp.nome_cor,
            dp.uf_nascimento,
            dp.nome_municipio_nascimento,
            uo.codigounidade as codigounidade_alternativa,
            uo.caminho_unidade as caminho_unidade_alternativa,
            uo.codigounidadepai as codigounidadepai_alternativa,
            uo.ordem_grandeza as ordem_grandeza_alternativa,
            substring(df.cod_funcao, 1, 1) || substring(
                df.cod_funcao, length(df.cod_funcao) - 2, 3
            ) as codigo_combinacao_siape
        from {{ ref("dados_funcionais") }} as df
        left join {{ ref("dados_pessoais") }} as dp on df.cpf = dp.cpf
        left join
            {{ ref("unidade_organizacional") }} as uo
            on df.sigla_uorg_exercicio = uo.sigla
        where dt_ocorr_aposentadoria is null and cod_funcao is not null
    ),

    -- select count(*) from codigos_siape;
    codigo_siorg_combinado as (
        select
            *,
            substring(funcao_sigla, 1, 1) || substring(
                funcao_sigla, length(funcao_sigla) - 2, 3
            ) as codigo_combinacao_siorg
        from codigos_siorg
    ),

    numeracao_cargos_siape as (
        select
            *,
            row_number() over (
                partition by codigo_combinacao_siape, sigla_uorg_exercicio
                order by sigla_uorg_exercicio
            ) as ordem_siape
        from codigos_siape
    ),

    numeracao_cargos_siorg as (
        select
            *,
            row_number() over (
                partition by codigo_combinacao_siorg, siglaunidade order by siglaunidade
            ) as ordem_siorg
        from codigo_siorg_combinado
    ),

    primeira_correlacao as (
        select
            *,
            case
                when
                    siorg.codigo_combinacao_siorg is not null
                    and siape.codigo_combinacao_siape is not null
                then 'inner'
                when
                    siorg.codigo_combinacao_siorg is not null
                    and siape.codigo_combinacao_siape is null
                then 'left'
                when
                    siorg.codigo_combinacao_siorg is null
                    and siape.codigo_combinacao_siape is not null
                then 'right'
            end as tipo_correlacao
        from numeracao_cargos_siorg as siorg
        full join
            numeracao_cargos_siape as siape
            on siorg.codigo_combinacao_siorg = siape.codigo_combinacao_siape
            and siorg.siglaunidade = siape.sigla_uorg_exercicio
            and siorg.ordem_siorg = siape.ordem_siape
    ),

    -- select count(*) from primeira_correlacao
    tabela_correlacao_cargos as (
        select distinct
            pr.cod_funcao as codigo_siape,
            pr.funcao_sigla as codigo_siorg,
            pr.codigo_combinacao_siape,
            pr.codigo_combinacao_siorg,
            pr.matricula_siape as matricula_siape,
            pr.cpf as cpf,
            pr.cpf_chefia_imediata as cpf_chefia_imediata,
            pr.cod_situacao_funcional as cod_situacao_funcional,
            pr.nome_situacao_funcional as nome_situacao_funcional,
            pr.hierarquia_cargo as hierarquia_cargo,
            pr.nome_pessoa as servidor,
            pr.dt_nascimento as dt_nascimento,
            pr.nome_sexo as nome_sexo,
            pr.nome_estado_civil as nome_estado_civil,
            pr.nome_nacionalidade as nome_nacionalidade,
            pr.nome_cor as nome_cor,
            pr.uf_nascimento as uf_nascimento,
            pr.nome_municipio_nascimento as nome_municipio_nascimento,
            dp.nome_pessoa as nome_chefia,
            coalesce(
                cast(pr.codigounidade as text), cast(pr.codigounidade_alternativa as text)
            ) as codigounidade,
            coalesce(
                cast(pr.codigounidadepai as text),
                cast(pr.codigounidadepai_alternativa as text)
            ) as codigounidadepai,
            coalesce(
                cast(pr.caminho_unidade as text),
                cast(pr.caminho_unidade_alternativa as text)
            ) as caminho_unidade,
            coalesce(
                cast(pr.ordem_grandeza as text),
                cast(pr.ordem_grandeza_alternativa as text)
            ) as ordem_grandeza,
            coalesce(nomeunidade, nome_uorg_exercicio) as nomeunidade,
            coalesce(siglaunidade, sigla_uorg_exercicio) as siglaunidade,
            coalesce(denominacao, nome_cargo) as nome_cargo,
            case
                when cod_situacao_funcional = '04' then 'Nomeação livre' else 'Carreira'
            end as servidores_carreira
        from primeira_correlacao as pr
        left join {{ ref("dados_pessoais") }} as dp on pr.cpf_chefia_imediata = dp.cpf
        order by caminho_unidade, hierarquia_cargo
    )

select *
from tabela_correlacao_cargos
