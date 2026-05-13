{{ config(materialized="table") }}

with
    deputados_raw as (
        select
            id::integer as id,
            nome::text as nome,
            siglapartido::text as sigla_partido,
            uripartido::text as uri_partido,
            siglauf::text as sigla_uf,
            idlegislatura::integer as id_legislatura,
            datahora::timestamptz as data_evento,
            trim(situacao)::text as situacao,
            condicaoeleitoral::text as condicao_eleitoral,
            parlamentar_id::integer as parlamentar_id,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("camara_deputados", "deputados_historico") }}
        where situacao is not null 
          and situacao != ''
    ),

    -- Legislatura Atual para evitar vazamentos de estado ativo em mandatos extintos
    meta_legislatura as (
        select max(id_legislatura) as max_id_leg from deputados_raw
    ),

    legislaturas_dim as (
        select
            id,
            data_fim::timestamptz as data_fim_legislatura
        from {{ ref("legislaturas") }}
    ),

    calculo_periodos as (
        select
            dr.*,
            lead(dr.data_evento) over (
                partition by dr.id 
                order by dr.data_evento asc
            ) as proximo_evento_data,
            ml.max_id_leg,
            ld.data_fim_legislatura
        from deputados_raw dr
        cross join meta_legislatura ml
        left join legislaturas_dim ld
            on dr.id_legislatura = ld.id
    )

select
    id,
    nome,
    sigla_partido,
    id_legislatura,
    situacao,
    data_evento as data_filiacao,
    case 
        -- Caso 1: Existe um próximo registro (segue a cronologia normal)
        when proximo_evento_data is not null then proximo_evento_data

        -- Caso 2: É o último registro, mas a situação é de encerramento explícito (Vacância/Fim de Mandato/Falecimento)
        when situacao in ('Vacância', 'Fim de Mandato', 'Falecimento') then data_evento

        -- Caso 3: É o último registro, restando qualquer outra situação, mas de uma LEGISLATURA ANTIGA
        when id_legislatura < max_id_leg then coalesce(data_fim_legislatura, data_evento)

        -- Caso 4: É o último registro, restando qualquer outra situação, na legislatura ATUAL
        when id_legislatura = max_id_leg then null

        else null 
    end as data_desfiliacao,
    dt_ingest
from calculo_periodos