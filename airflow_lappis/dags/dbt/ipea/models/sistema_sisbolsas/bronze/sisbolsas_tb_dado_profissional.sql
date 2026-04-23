{{ config(materialized="table") }}

with
    sisbolsas_tb_dado_profissional as (
        select
            co_dado_profissional::text as co_dado_profissional,
            tp_situacao_funcional::text as tp_situacao_funcional,
            {{ safe_boolean('in_vinculo') }} as in_vinculo,
            tp_setor::text as tp_setor,
            {{ safe_boolean('in_funcao_gratificada') }} as in_funcao_gratificada,
            ds_instituicao::text as ds_instituicao,
            ds_empregador::text as ds_empregador,
            tp_cargo::text as tp_cargo,
            co_usuario::text as co_usuario,
            dt_criacao::text as dt_criacao
        from {{ source("sisbolsas", "tb_dado_profissional") }}
    )

select *
from sisbolsas_tb_dado_profissional
