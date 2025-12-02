with recursive
    fonte as (
        select
            regexp_replace(codigounidade, '^.*/', '') as codigounidade,
            regexp_replace(codigounidadepai, '^.*/', '') as codigounidadepai,
            regexp_replace(codigoorgaoentidade, '^.*/', '') as codigoorgaoentidade,
            regexp_replace(codigotipounidade, '^.*/', '') as codigotipounidade,
            nome,
            sigla,
            regexp_replace(codigoesfera, '^.*/', '') as codigoesfera,
            regexp_replace(codigopoder, '^.*/', '') as codigopoder,
            regexp_replace(codigonaturezajuridica, '^.*/', '') as codigonaturezajuridica,
            codigosubnaturezajuridica,
            nivelnormatizacao,
            versaoconsulta,
            datafinalversaoconsulta,
            operacao,
            codigounidadepaianterior,
            codigoorgaoentidadeanterior
        from {{ ref("unidade_organizacional") }}
    ),

    unidades_raiz as (select '7' as codigounidade_raiz),

    hierarquia_unidade_organizacional as (
        select f.*, 1 as ordem_grandeza, sigla as caminho_unidade
        from fonte as f
        inner join unidades_raiz as r on f.codigounidade = r.codigounidade_raiz

        union all

        select
            f.*,
            h.ordem_grandeza + 1 as ordem_grandeza,
            h.caminho_unidade || '-' || lpad(f.sigla::text, 5, '0') as caminho_unidade
        from fonte as f
        inner join
            hierarquia_unidade_organizacional as h on f.codigounidadepai = h.codigounidade
    ),

    codigos_siorg as (
        select distinct
            eorg.nomeunidade,
            eorg.codigounidade,
            eorg.ordem_grandeza,
            eorg.denominacao,
            uo.codigounidadepai,
            uo.caminho_unidade,
            replace(eorg.funcao, ' ', '') as funcao,
            case
                when eorg.siglaunidade = 'GABIN-IPEA' then 'GABIN' else siglaunidade
            end as siglaunidade,
            substring(funcao, -3, 4)
            || substring(funcao, length(funcao) - 3, 1) as categoria_cargo,
            -- hierarquia do cargo está sendo definida a partir da fórmula:
            -- (categoria do cargo * 1000) - nível do cargo
            -- quanto menor a hierarquia, maior o cargo
            right(funcao, 2) as nivel_cargo,
            (substring(funcao, -3, 4) || substring(funcao, length(funcao) - 3, 1))::int
            * 1000
            - (right(funcao, 2))::int as hierarquia_cargo
        from {{ ref("estrutura_organizacional_cargos") }} as eorg
        inner join
            hierarquia_unidade_organizacional as uo
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
            nome_pessoa,
            substring(df.cod_funcao, 1, 1) || substring(
                df.cod_funcao, length(df.cod_funcao) - 2, 3
            ) as codigo_combinacao_siape
        from {{ ref("dados_funcionais") }} as df
        left join {{ ref("dados_pessoais") }} as dp on df.cpf = dp.cpf
        where cod_funcao is not null and dt_ocorr_aposentadoria is null
    ),

    codigo_siorg_combinado as (
        select
            *,
            substring(funcao, 1, 1)
            || substring(funcao, length(funcao) - 2, 3) as codigo_combinacao_siorg
        from codigos_siorg
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
        from codigo_siorg_combinado as siorg
        full join
            codigos_siape as siape
            on siorg.codigo_combinacao_siorg = siape.codigo_combinacao_siape
            and siorg.siglaunidade = siape.sigla_uorg_exercicio
    ),

    tabela_correlacao_cargos as (
        select
            pr.cod_funcao as codigo_siape,
            pr.funcao as codigo_siorg,
            pr.codigounidade,
            pr.codigounidadepai,
            pr.caminho_unidade,
            pr.ordem_grandeza,
            pr.matricula_siape,
            pr.cpf,
            pr.cpf_chefia_imediata,
            pr.hierarquia_cargo,
            pr.nome_pessoa as servidor,
            dp.nome_pessoa as nome_chefia,
            coalesce(nomeunidade, nome_uorg_exercicio) as nomeunidade,
            coalesce(siglaunidade, sigla_uorg_exercicio) as siglaunidade,
            coalesce(denominacao, nome_cargo) as nome_cargo
        from primeira_correlacao as pr
        left join pessoas.dados_pessoais as dp on pr.cpf_chefia_imediata = dp.cpf
        order by caminho_unidade, hierarquia_cargo
    )

select *
from tabela_correlacao_cargos
