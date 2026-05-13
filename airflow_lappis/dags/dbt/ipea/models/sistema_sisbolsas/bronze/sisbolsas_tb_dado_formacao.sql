{{ config(materialized="table") }}

with
    sisbolsas_tb_dado_formacao as (
        select
            co_dado_formacao::text as co_dado_formacao,
            co_usuario::text as co_usuario,
            co_nivel_escolaridade::text as co_nivel_escolaridade,
            co_tipo_escolar::text as co_tipo_escolar,
            st_nivel_escolaridade::text as st_nivel_escolaridade,
            tp_nacionalidade_diploma::text as tp_nacionalidade_diploma,
            {{ safe_boolean('in_diploma_valido') }} as in_diploma_valido,
            co_area_formacao::text as co_area_formacao,
            ds_outra_formacao::text as ds_outra_formacao,
            ds_curso::text as ds_curso,
            ds_instituicao::text as ds_instituicao,
            co_estado::text as co_estado,
            ds_url_curriculo::text as ds_url_curriculo,
            dt_criacao::text as dt_criacao,
            co_pais::text as co_pais
        from {{ source("sisbolsas", "tb_dado_formacao") }}
    )

select *
from sisbolsas_tb_dado_formacao
