{{ config(materialized="table") }}

with
	tg_emendas_raw as (
		select
			-- Linhas de Restos a Pagar vem sem data de emissao aplicavel
			-- (emissao_mes/dia = '000/AAAA'); nesses casos a data fica nula.
			case
				when emissao_mes ~ '^[A-Z]{3}/[0-9]{4}$'
				then {{ target.schema }}.parse_date(emissao_mes)
			end as emissao_mes,
			case
				when emissao_dia ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
				then to_date(emissao_dia, 'DD/MM/YYYY')
			end as emissao_dia,
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
			localizador_gasto::text as localizador_gasto,
			localizador_gasto_descricao::text as localizador_gasto_descricao,
			regiao_pt::text as regiao_pt,
			case
    			when uf_pt = '-8' then regiao_pt
    			else uf_pt
			end as uf_pt,
			case
    			when uf_pt_descricao = 'SEM INFORMACAO' then regiao_pt
    			else uf_pt_descricao
			end::text as uf_pt_descricao,
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
			fonte_recursos_detalhada::text as fonte_recursos_detalhada,
			fonte_recursos_detalhada_descricao::text as fonte_recursos_detalhada_descricao,
			{{ parse_financial_value("despesas_empenhadas") }} as despesas_empenhadas,
            {{ parse_financial_value("despesas_liquidadas") }} as despesas_liquidadas,
            {{ parse_financial_value("despesas_pagas") }} as despesas_pagas,
            {{ parse_financial_value("restos_a_pagar_inscritos") }} as restos_a_pagar_inscritos,
            {{ parse_financial_value("restos_a_pagar_pagos") }} as restos_a_pagar_pagos,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("siafi", "ne_tesouro_emendas") }}
	)

select *
from tg_emendas_raw

