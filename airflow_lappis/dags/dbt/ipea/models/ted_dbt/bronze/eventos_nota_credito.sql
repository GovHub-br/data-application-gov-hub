-- Eventos contábeis das Notas de Crédito de TED (API de TED, endpoint evento):
-- PTRES, fonte, plano interno e natureza de despesa por NC. A fonte não tem
-- chave; a ingestão substitui a tabela inteira a cada carga.
with

    eventos_nota_credito as (
        select
            {{ safe_bigint("id_nota") }} as id_nota,
            {{ safe_text("cd_evento") }} as cd_evento,
            {{ safe_text("cd_ptres_evento") }} as cd_ptres,
            {{ safe_text("cd_fonte_recurso_evento") }} as cd_fonte_recurso,
            {{ safe_text("cd_plano_interno_evento") }} as cd_plano_interno,
            {{ safe_numeric("vl_evento") }} as vl_evento,
            {{ safe_text("cd_ug_responsavel_evento") }} as cd_ug_responsavel,
            {{ safe_text("codigo_natureza") }} as cd_natureza_despesa,
            {{ safe_text("descricao_natureza") }} as ds_natureza_despesa,
            {{ safe_text("nome_esfera_orcamentaria") }} as no_esfera_orcamentaria,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "eventos_nota_credito") }}
    )

select *
from eventos_nota_credito
