{{ config(materialized="table") }}

with
	tg_emendas_raw as (
		select
			{{ target.schema }}.parse_date(emissao_mes) as emissao_mes,
			to_date(emissao_dia, 'DD/MM/YYYY') as emissao_dia,
			programa_governo::integer as programa_governo,
			programa_governo_descricao::text as programa_governo_descricao,
			acao_governo::text as acao_governo,
			acao_governo_descricao::text as acao_governo_descricao,
			autor_emendas_orcamento::text as autor_emendas_orcamento,
			autor_emendas_orcamento_descricao::text as autor_emendas_orcamento_descricao,
			initcap(
				trim(
					regexp_replace(
						split_part(autor_emendas_orcamento_descricao, '/', 1),
						'\s+',
						' ',
						'g'
					)
				)
			) as autor_emendas_orcamento_nome,
			uf_pt::text as uf_pt,
			uf_pt_descricao::text as uf_pt_descricao,
			municipio_pt::text as municipio_pt,
			ne_ccor::text as ne_ccor,
			ne_num_processo::text as ne_num_processo,
			ne_info_complementar::text as ne_info_complementar,
			ne_ccor_descricao::text as ne_ccor_descricao,
			doc_observacao::text as doc_observacao,
			grupo_despesa::integer as grupo_despesa,
			grupo_despesa_descricao::text as grupo_despesa_descricao,
			natureza_despesa::text as natureza_despesa,
			natureza_despesa_descricao::text as natureza_despesa_descricao,
			modalidade_aplicacao::integer as modalidade_aplicacao,
			modalidade_aplicacao_descricao::text as modalidade_aplicacao_descricao,
			ne_ccor_favorecido::text as ne_ccor_favorecido,
			ne_ccor_favorecido_descricao::text as ne_ccor_favorecido_descricao,
			ne_ccor_ano_emissao::integer as ne_ccor_ano_emissao,
			ptres::integer as ptres,
			item_informacao::text as item_informacao,
			item_informacao_descricao::text as item_informacao_descricao,
			{{ parse_financial_value("despesas_empenhadas") }} as despesas_empenhadas,
            {{ parse_financial_value("despesas_liquidadas") }} as despesas_liquidadas,
            {{ parse_financial_value("despesas_pagas") }} as despesas_pagas,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("siafi", "ne_tesouro_emendas") }}
	)

select *
from tg_emendas_raw

