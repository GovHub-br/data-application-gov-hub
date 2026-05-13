{{ config(materialized="table", alias="pf_ptres_mir") }}


with
	pf_ptres_raw as (
		select
			programa_governo::text as programa_governo,
			programa_governo_descricao::text as programa_governo_descricao,
			plano_orcamentario::text as plano_orcamentario,
			plano_orcamentario_descricao_1::text as plano_orcamentario_descricao_1,
			plano_orcamentario_descricao_2::text as plano_orcamentario_descricao_2,
			plano_orcamentario_descricao_3::text as plano_orcamentario_descricao_3,
			plano_orcamentario_descricao_4::text as plano_orcamentario_descricao_4,
			plano_orcamentario_descricao_5::text as plano_orcamentario_descricao_5,
			plano_orcamentario_descricao_6::text as plano_orcamentario_descricao_6,
			acao_governo::text as acao_governo,
			acao_governo_descricao::text as acao_governo_descricao,
			ptres::text as ptres,
			natureza_despesa::text as natureza_despesa,
			natureza_despesa_descricao::text as natureza_despesa_descricao,
			{{ parse_financial_value("dotacao_inicial") }} as dotacao_inicial,
			{{ parse_financial_value("dotacao_suplementar") }} as dotacao_suplementar,
			{{ parse_financial_value("dotacao_atualizada") }} as dotacao_atualizada,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("siafi", "programacao_acao_ptres") }}
	)

select *
from pf_ptres_raw
