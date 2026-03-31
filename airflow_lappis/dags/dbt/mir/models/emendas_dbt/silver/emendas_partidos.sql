{{ config(materialized='table') }}

WITH tg_emendas AS (
	SELECT *
	FROM {{ ref('tg_emendas') }}
),

parlamentares AS (
	SELECT *
	FROM {{ ref('parlamentares') }}
),

tg_emendas_tratado AS (
	SELECT
		*,
		TRIM(UPPER(autor_emendas_orcamento_nome)) AS chave_join_nome
	FROM tg_emendas
),

final AS (
	SELECT
		e.emissao_mes,
		e.emissao_dia,
		e.programa_governo,
		e.programa_governo_descricao,
		e.acao_governo,
		e.acao_governo_descricao,
		e.autor_emendas_orcamento,
		e.autor_emendas_orcamento_descricao,
		e.autor_emendas_orcamento_nome,
		e.uf_pt,
		e.uf_pt_descricao,
		e.municipio_pt,
		e.ne_ccor,
		e.ne_num_processo,
		e.ne_info_complementar,
		e.ne_ccor_descricao,
		e.doc_observacao,
		e.grupo_despesa,
		e.grupo_despesa_descricao,
		e.natureza_despesa,
		e.natureza_despesa_descricao,
		e.modalidade_aplicacao,
		e.modalidade_aplicacao_descricao,
		e.ne_ccor_favorecido,
		e.ne_ccor_favorecido_descricao,
		e.ne_ccor_ano_emissao,
		e.ptres,
		e.item_informacao,
		e.item_informacao_descricao,
		e.despesas_empenhadas,
		e.despesas_liquidadas,
		e.despesas_pagas,

		p.id_parlamentar,
		p.cargo_parlamentar,
		p.nome_parlamentar,
		p.sigla_partido,
		p.uf_parlamentar,
		p.url_foto,
		p.email,
		p.url_logo_partido,

		e.dt_ingest

	FROM tg_emendas_tratado e
	LEFT JOIN parlamentares p
		ON e.chave_join_nome = p.chave_join_nome
)

SELECT *
FROM final
