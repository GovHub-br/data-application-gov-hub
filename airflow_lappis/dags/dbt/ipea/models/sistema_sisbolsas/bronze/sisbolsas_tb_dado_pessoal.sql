{{ config(materialized="table") }}

with
    sisbolsas_tb_dado_pessoal as (
        select
            co_dado_pessoal::text as co_dado_pessoal,
            co_usuario::text as co_usuario,
            regexp_replace(ds_cpf, '[^0-9]', '', 'g')::text as ds_cpf,
            dt_nascimento::text as dt_nascimento,
            co_estado_civil::text as co_estado_civil,
            tp_sexo::text as tp_sexo,
            tp_nacionalidade::text as tp_nacionalidade,
            ds_naturalidade::text as ds_naturalidade,
            ds_rg::text as ds_rg,
            dt_emissao_rg::text as dt_emissao_rg,
            ds_orgao_emissor::text as ds_orgao_emissor,
            co_estado_orgao::text as co_estado_orgao,
            ds_passaporte::text as ds_passaporte,
            tp_visto::text as tp_visto,
            dt_validade_visto::text as dt_validade_visto,
            nu_telefone_principal::text as nu_telefone_principal,
            nu_telefone_alternativo::text as nu_telefone_alternativo,
            dt_criacao::text as dt_criacao,
            co_pais_passaporte::text as co_pais_passaporte,
            ds_ddd_principal::text as ds_ddd_principal,
            ds_ddd_alternativo::text as ds_ddd_alternativo,
            co_etnia::text as co_etnia,
            ds_email_alternativo::text as ds_email_alternativo,
            tp_fator::text as tp_fator,
            tp_sanguineo::text as tp_sanguineo
        from {{ source("sisbolsas", "tb_dado_pessoal") }}
    )

select *
from sisbolsas_tb_dado_pessoal
