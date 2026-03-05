{{ config(materialized="table") }}

with
    ordens_bancarias_raw as (
        select
            id_op_ob::integer as id_op_ob,
            data_emissao_op::date as data_emissao_op,
            numero_ordem_pagamento::text as numero_ordem_pagamento,
            vinculacao_op::integer as vinculacao_op,
            situacao_op::integer as situacao_op,
            descricao_situacao_op::text as descricao_situacao_op,
            data_situacao_op::date as data_situacao_op,
            data_emissao_ob::date as data_emissao_ob,
            numero_ordem_bancaria::text as numero_ordem_bancaria,
            numero_ordem_lancamento::text as numero_ordem_lancamento,
            data_assinatura_ordenador_despesa_ob::date as data_assinatura_ordenador_despesa_ob,
            data_assinatura_gestor_financeiro_ob::date as data_assinatura_gestor_financeiro_ob,
            id_dh::integer as id_dh,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "ordens_bancarias_especiais") }}
    )  --

select *
from ordens_bancarias_raw
