-- Modelo intermediário que centraliza os enriquecimentos de dados de servidores
-- Combina informações de hierarquia, dados funcionais, organizacionais e pessoais
-- Este modelo evita duplicação de código nos modelos gold
with
    hierarquia_enriquecida as (
        select
            ph.codigo_siape,
            ph.codigo_siorg,
            ph.codigo_combinacao_siape,
            ph.codigo_combinacao_siorg,
            ph.matricula_siape,
            ph.cpf,
            ph.cpf_chefia_imediata,
            ph.cod_situacao_funcional,
            ph.nome_situacao_funcional,
            ph.hierarquia_cargo,
            ph.servidor,
            ph.dt_nascimento,
            ph.nome_sexo,
            ph.nome_estado_civil,
            ph.nome_nacionalidade,
            ph.nome_cor,
            ph.uf_nascimento,
            ph.nome_municipio_nascimento,
            ph.nome_chefia,
            ph.codigounidade,
            ph.codigounidadepai,
            ph.caminho_unidade,
            ph.ordem_grandeza,
            ph.nomeunidade,
            ph.siglaunidade,
            ph.nome_cargo,
            ph.servidores_carreira,
            ph.dt_ingest as dt_ingest_ph,
            df.dt_ingest as dt_ingest_df,
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
        select distinct ph.*, du.nome_municipio_uorg, du.dt_ingest as dt_ingest_du
        from hierarquia_enriquecida ph
        inner join {{ ref("dados_uorg") }} du on ph.siglaunidade = du.sigla_uorg
        order by caminho_unidade, hierarquia_cargo
    )

select distinct
        se.codigo_siape,
    se.codigo_siorg,
    se.codigo_combinacao_siape,
    se.codigo_combinacao_siorg,
    se.matricula_siape,
    se.cpf,
    se.cpf_chefia_imediata,
    se.cod_situacao_funcional,
    se.nome_situacao_funcional,
    se.hierarquia_cargo,
    se.servidor,
    se.dt_nascimento,
    se.nome_sexo,
    se.nome_estado_civil,
    se.nome_nacionalidade,
    se.nome_cor,
    se.uf_nascimento,
    se.nome_municipio_nascimento,
    se.nome_chefia,
    se.codigounidade,
    se.codigounidadepai,
    se.caminho_unidade,
    se.ordem_grandeza,
    se.nomeunidade,
    se.siglaunidade,
    se.nome_cargo,
    se.servidores_carreira,
    se.pdg,
    se.unidade_exercicio,
    se.nome_municipio_uorg,
    sd.cod_escolaridade_principal,
    sd.nome_escolaridade_principal,
    sd.nome_deficiencia_fisica,
    sd.nome_cargo as nome_cargo_emprego,
    greatest(se.dt_ingest_ph, se.dt_ingest_df, se.dt_ingest_du, sd.dt_ingest) as dt_ingest
from servidores_enriquecidos se
inner join {{ ref("servidores_detalhados") }} sd on se.cpf = sd.cpf
