{{ config(materialized="table") }}

with
    bronze_planos_acoes as (select * from {{ ref("planos_acoes") }}),

    bronze_deputados as (select * from {{ ref("deputados") }}),

    bronze_senadores as (select * from {{ ref("senadores") }}),

    parlamentares_unificados as (
        select
            id as id_parlamentar,
            trim(upper(nome)) as chave_join_nome,
            nome as nome_parlamentar,
            'Deputado' as cargo_parlamentar,
            siglapartido as sigla_partido,
            siglauf as uf_parlamentar,
            urlfoto as url_foto,
            email
        from bronze_deputados

        union all

        select
            id as id_parlamentar,
            trim(upper(nome_parlamentar)) as chave_join_nome,
            nome_parlamentar as nome_parlamentar,
            'Senador' as cargo_parlamentar,
            sigla_partido as sigla_partido,
            uf as uf_parlamentar,
            url_foto as url_foto,
            email
        from bronze_senadores
    ),

    planos_acoes_tratado as (
        select *, trim(upper(nome_parlamentar_emenda_plano_acao)) as chave_join_nome
        from bronze_planos_acoes
    ),

    final as (
        select
            -- Todas as features de Planos de Ação
            pa.id_plano_acao,
            pa.codigo_plano_acao,
            pa.ano_plano_acao,
            pa.modalidade_plano_acao,
            pa.situacao_plano_acao,
            pa.cnpj_beneficiario_plano_acao,
            pa.nome_beneficiario_plano_acao,
            pa.uf_beneficiario_plano_acao,
            pa.codigo_banco_plano_acao,
            pa.codigo_situacao_dado_bancario_plano_acao,
            pa.nome_banco_plano_acao,
            pa.numero_agencia_plano_acao,
            pa.dv_agencia_plano_acao,
            pa.numero_conta_plano_acao,
            pa.dv_conta_plano_acao,
            pa.nome_parlamentar_emenda_plano_acao,
            pa.ano_emenda_parlamentar_plano_acao,
            pa.codigo_parlamentar_emenda_plano_acao,
            pa.sequencial_emenda_parlamentar_plano_acao,
            pa.numero_emenda_parlamentar_plano_acao,
            pa.codigo_emenda_parlamentar_formatado_plano_acao,
            pa.codigo_descricao_areas_politicas_publicas_plano_acao,
            pa.descricao_programacao_orcamentaria_plano_acao,
            pa.motivo_impedimento_plano_acao,
            pa.valor_custeio_plano_acao,
            pa.valor_investimento_plano_acao,
            pa.id_programa,

            -- Features unificadas dos Parlamentares
            parl.id_parlamentar,
            parl.cargo_parlamentar,
            parl.nome_parlamentar,
            parl.sigla_partido,
            parl.uf_parlamentar,
            parl.url_foto,
            parl.email,

            -- Data de ingestão (mantendo a da tabela fato)
            pa.dt_ingest

        from planos_acoes_tratado pa
        left join
            parlamentares_unificados parl on pa.chave_join_nome = parl.chave_join_nome
    )

select *
from final
