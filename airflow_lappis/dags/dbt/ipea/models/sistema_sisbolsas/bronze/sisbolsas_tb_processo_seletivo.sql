{{ config(materialized="table") }}

with
    sisbolsas_tb_processo_seletivo as (
        select
            co_processo_seletivo::text as co_processo_seletivo,
            co_usuario::text as co_usuario,
            co_dado_formacao::text as co_dado_formacao,
            co_selecao::text as co_selecao,
            co_dado_profissional::text as co_dado_profissional,
            co_situacao_concessao::text as co_situacao_concessao,
            st_processo_seletivo::text as st_processo_seletivo,
            dt_criacao::text as dt_criacao,
            {{ safe_boolean('in_nao_apto') }} as in_nao_apto,
            {{ safe_boolean('in_classificacao') }} as in_classificacao,
            tx_observacao_avaliacao::text as tx_observacao_avaliacao,
            tx_observacao_classificacao::text as tx_observacao_classificacao,
            {{ safe_numeric('vl_total_criterio') }} as vl_total_criterio,
            {{ safe_numeric('vl_total_geral') }} as vl_total_geral,
            tx_observacao_entrevista::text as tx_observacao_entrevista,
            ds_instituicao_bolsa::text as ds_instituicao_bolsa,
            nu_posicao::text as nu_posicao,
            ds_token_aceite::text as ds_token_aceite,
            {{ safe_boolean('in_bolsa_ativa') }} as in_bolsa_ativa,
            ds_institu_bolsa_ativa::text as ds_institu_bolsa_ativa,
            {{ safe_boolean('in_declaracao_veracidade') }} as in_declaracao_veracidade
        from {{ source("sisbolsas", "tb_processo_seletivo") }}
    )

select *
from sisbolsas_tb_processo_seletivo
