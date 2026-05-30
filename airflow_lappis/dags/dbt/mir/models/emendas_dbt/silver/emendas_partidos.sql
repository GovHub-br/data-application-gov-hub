{{ config(materialized="table") }}

with
    tg_emendas as (select * from {{ ref("tg_emendas") }}),

    parlamentares_hist as (select * from {{ ref("parlamentares_historico") }}),

    tg_emendas_tratado as (
        select
            *,
            {{ name_formater("autor_emendas_orcamento_nome") }} as chave_join_nome,
            row_number() over () as emenda_id
        from tg_emendas
    ),

    cruzamento_bruto as (
        select
            e.emissao_mes,
            e.emissao_dia,
            e.programa_governo as codigo_programa,
            e.programa_governo_descricao as programa,
            e.acao_governo as codigo_acao_ajustada,
            e.acao_governo_descricao as acao_ajustada,
            e.autor_emendas_orcamento_descricao,
            e.localizador_gasto as localizador_gasto,
            e.localizador_gasto_descricao as localizador_gasto_descricao,
            e.regiao_pt as regiao_pt,
            e.uf_pt as uf,
            e.uf_pt_descricao as uf_descricao,
            e.municipio_pt as municipio,
            'Brasil' as pais,
            e.ne_ccor,
            e.ne_num_processo,
            e.ne_info_complementar,
            e.ne_ccor_descricao,
            e.doc_observacao,
            e.grupo_despesa as codigo_gnd,
            e.grupo_despesa_descricao as gnd,
            e.natureza_despesa,
            e.natureza_despesa_descricao,
            e.modalidade_aplicacao as codigo_modalidade,
            e.modalidade_aplicacao_descricao as modalidade,
            e.ne_ccor_favorecido,
            e.ne_ccor_favorecido_descricao,
            e.ne_ccor_ano_emissao,
            e.ptres,
            e.item_informacao,
            e.item_informacao_descricao,
            e.despesas_empenhadas,
            e.despesas_liquidadas,
            e.despesas_pagas,
            e.autor_emendas_orcamento_nome,

            e.emenda_id,

            p.id_parlamentar as id_autor,
            p.cargo_parlamentar as cargo_autor,
            p.nome_parlamentar as autor,
            p.sigla_partido as partido,
            p.uf_parlamentar as uf_autor,
            p.url_foto as url_foto_autor,
            p.email as email_autor,
            p.url_logo_partido as url_foto_partido,

            e.dt_ingest,

            -- Prioridade de cruzamento
            case
                when
                    e.emissao_dia >= p.data_filiacao::date
                    and e.emissao_dia <= coalesce(p.data_desfiliacao::date, current_date)
                then 1
                -- Se achou nome, mas a data não bateu
                when p.id_parlamentar is not null
                then 2
                -- Nomes que nem existem
                else 3
            end as prioridade_match,

            -- Distância de fallback para quando não tivermos batido o range
            least(
                abs(extract(epoch from (e.emissao_dia::timestamptz - p.data_filiacao))),
                abs(
                    extract(
                        epoch
                        from
                            (
                                e.emissao_dia::timestamptz
                                - coalesce(p.data_desfiliacao, current_timestamp)
                            )
                    )
                )
            ) as distancia_tempo

        from tg_emendas_tratado e
        left join parlamentares_hist p on e.chave_join_nome = p.chave_join_nome
    ),

    deduplicado as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by emenda_id
                        order by prioridade_match asc, distancia_tempo asc
                    ) as rn
                from cruzamento_bruto
            ) sub
        where rn = 1
    )

select
    emissao_mes,
    emissao_dia,
    codigo_programa,
    programa,
    codigo_acao_ajustada,
    acao_ajustada,
    autor_emendas_orcamento_descricao,
    autor_emendas_orcamento_nome,
    localizador_gasto,
    localizador_gasto_descricao,
    regiao_pt,
    uf,
    uf_descricao,
    municipio,
    pais,
    ne_ccor,
    ne_num_processo,
    ne_info_complementar,
    ne_ccor_descricao,
    doc_observacao,
    codigo_gnd,
    gnd,
    natureza_despesa,
    natureza_despesa_descricao,
    codigo_modalidade,
    modalidade,
    ne_ccor_favorecido,
    ne_ccor_favorecido_descricao,
    ne_ccor_ano_emissao,
    ptres,
    item_informacao,
    item_informacao_descricao,
    despesas_empenhadas,
    despesas_liquidadas,
    despesas_pagas,

    id_autor,
    cargo_autor,
    autor,
    partido,
    uf_autor,
    url_foto_autor,
    email_autor,
    url_foto_partido,

    dt_ingest
from deduplicado
