{{ config(materialized="table") }}

with
    executor_especial_raw as (
        select
            id_plano_acao::integer as id_plano_acao,
            id_executor::integer as id_executor,
            cnpj_executor::text as cnpj_executor,
            nome_executor::text as nome_executor,
            objeto_executor::text as objeto_executor,
            vl_custeio_executor::numeric(15, 2) as valor_custeio_executor,
            vl_investimento_executor::numeric(15, 2) as valor_investimento_executor,
            ind_recursos_gerenciados_conta_especifica_executor::text as ind_recursos_gerenciados_conta_especifica_executor,
            codigo_banco_executor::text as codigo_banco_executor,
            nome_banco_executor::text as nome_banco_executor,
            nullif(numero_agencia_executor, 'NaN')::numeric::integer as numero_agencia_executor,
            dv_agencia_executor::text as dv_agencia_executor,
            nome_agencia_executor::text as nome_agencia_executor,
            numero_conta_executor::text as numero_conta_executor,
            dv_conta_executor::text as dv_conta_executor,
            nullif(codigo_situacao_dado_bancario_executor, 'NaN')::numeric::integer as codigo_situacao_dado_bancario_executor,
            descricao_situacao_dado_bancario_executor::text as descricao_situacao_dado_bancario_executor,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transferegov_emendas", "executor_especial") }}
    )

select *
from executor_especial_raw