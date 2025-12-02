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
            df.modalidade_pgd,
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
        where dt_ocorr_aposentadoria is null and dt_ocorr_exclusao is null
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

    -- select count(*) from primeira_correlacao
    tabela_correlacao_cargos as (
        select distinct
            pr.cod_funcao as codigo_siape,
            pr.funcao_sigla as codigo_siorg,
            pr.codigo_combinacao_siape,
            pr.codigo_combinacao_siorg,
            pr.matricula_siape,
            pr.cpf,
            pr.cpf_chefia_imediata,
            pr.cod_situacao_funcional,
            pr.nome_situacao_funcional,
            pr.hierarquia_cargo,
            pr.nome_pessoa as servidor,
            pr.dt_nascimento,
            pr.nome_sexo,
            pr.nome_estado_civil,
            pr.nome_nacionalidade,
            pr.nome_cor,
            pr.uf_nascimento,
            pr.nome_municipio_nascimento,
            pr.modalidade_pgd,
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
    ),

    hierarquia_enriquecida as (
        select
            ph.*,
            case
                when ph.modalidade_pgd is null
                then 'Não participa'
                when ph.modalidade_pgd = 'parcial'
                then 'Parcial'
                when ph.modalidade_pgd = 'integral'
                then 'Integral'
                when ph.modalidade_pgd = 'presencial'
                then 'Presencial'
                when ph.modalidade_pgd = 'no exterior'
                then 'No exterior'
            end as pdg,
            case
                when ph.nome_situacao_funcional = 'ATIVO EM OUTRO ORGAO'
                then 'Ativo em outro órgão'
                else ph.siglaunidade
            end as unidade_exercicio
        from tabela_correlacao_cargos as ph
    ),

    servidores_enriquecidos as (
        select distinct ph.*, du.nome_municipio_uorg
        from hierarquia_enriquecida as ph
        left join {{ ref("dados_uorg") }} as du on ph.siglaunidade = du.sigla_uorg
        order by caminho_unidade, hierarquia_cargo
    )

select distinct
    se.*,
    cod_escolaridade_principal,
    nome_escolaridade_principal,
    nome_deficiencia_fisica,
    sd.nome_cargo as nome_cargo_emprego
from servidores_enriquecidos as se
left join {{ ref("servidores_detalhados") }} as sd on se.cpf = sd.cpf
