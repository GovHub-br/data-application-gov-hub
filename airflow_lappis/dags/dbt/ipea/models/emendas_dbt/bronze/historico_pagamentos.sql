{{ config(materialized="table") }}

with
    historico_pagamentos_raw as (
        select
            id_historico_op_ob::integer as id_historico_op_ob,
            data_hora_historico_op::timestamp as data_hora_historico_op,
            historico_situacao_op::integer as historico_situacao_op,
            descricao_historico_situacao_op::text as descricao_historico_situacao_op,
            id_op_ob::integer as id_op_ob,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "historico_pagamentos_especiais") }}
    )  --

select *
from historico_pagamentos_raw
