{{ config(materialized="table", alias="pf_tesouro_mir") }}


with
	pf_tesouro_raw as (
		select
			emissao_mes::text as emissao_mes,
			to_date(emissao_dia, 'DD/MM/YYYY') as emissao_dia,
			ug_emitente::text as ug_emitente,
			ug_emitente_descricao::text as ug_emitente_descricao,
			ug_favorecido::text as ug_favorecido,
			ug_favorecido_descricao::text as ug_favorecido_descricao,
			pf_evento::text as pf_evento,
			pf_evento_descricao::text as pf_evento_descricao,
			pf::text as pf,
			pf_inscricao::text as pf_inscricao,
			pf_acao::text as pf_acao,
			pf_acao_descricao::text as pf_acao_descricao,
			pf_fonte_recursos::text as pf_fonte_recursos,
			pf_fonte_recursos_descricao::text as pf_fonte_recursos_descricao,
			doc_observacao::text as doc_observacao,
			{{ parse_financial_value("pf_valor_linha") }} as pf_valor_linha,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("siafi", "pf_tesouro") }}
	)

select *
from pf_tesouro_raw
