{{ config(materialized="table") }}

with
    proposta as (
        select *
        from {{ ref("proposta") }}
    ),
    historico_situacao as (
        select *
        from {{ ref("historico_situacao") }}
    )

select
    p.*,
    h.dia_historico_sit,
    h.historico_sit,
    h.dias_historico_sit,
    h.cod_historico_sit
from proposta p
left join historico_situacao h on p.id_proposta = h.id_proposta