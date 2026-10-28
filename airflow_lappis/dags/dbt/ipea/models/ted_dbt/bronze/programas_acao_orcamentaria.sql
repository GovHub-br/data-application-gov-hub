-- Ações orçamentárias dos programas de TED (API de TED, endpoint
-- programa_acao_orcamentaria). A fonte não tem chave; a ingestão substitui a
-- tabela inteira a cada carga.
with

    programas_acao_orcamentaria as (
        select distinct
            {{ safe_bigint("id_programa") }} as id_programa,
            {{ safe_text("tx_codigo_acao_orcamentaria") }} as cd_acao_orcamentaria,
            {{ safe_text("tx_descricao_acao_orcamentaria") }} as ds_acao_orcamentaria
        from {{ source("transfere_gov", "programas_acao_orcamentaria") }}
    )

select *
from programas_acao_orcamentaria
