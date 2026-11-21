-- TRF das Programações Financeiras de TED (API de TED, endpoint trf): vinculação,
-- fonte, categoria de gasto e valor. A fonte não tem chave; a ingestão substitui
-- a tabela inteira a cada carga.
with

    trf_programacao_financeira as (
        select
            {{ safe_bigint("id_programacao") }} as id_programacao,
            {{ safe_text("cd_vinculacao_trf") }} as cd_vinculacao,
            {{ safe_text("cd_fonte_recurso_trf") }} as cd_fonte_recurso,
            {{ safe_text("cd_categoria_gasto_trf") }} as cd_categoria_gasto,
            {{ safe_numeric("vl_valor_trf") }} as vl_trf,
            {{ safe_text("cd_situacao_contabil_trf") }} as cd_situacao_contabil,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "trf_programacao_financeira") }}
    )

select *
from trf_programacao_financeira
