{{ config(materialized='table') }}

WITH tg_emendas AS (
	SELECT *
	FROM {{ ref('tg_emendas') }}
),

parlamentares_hist AS (
	SELECT *
	FROM {{ ref('parlamentares_historico') }}
),

tg_emendas_tratado AS (
	SELECT
		*,
		{{ name_formater("autor_emendas_orcamento_nome") }} AS chave_join_nome,
		ROW_NUMBER() OVER () as emenda_id
	FROM tg_emendas
),

cruzamento_bruto AS (
	SELECT
		e.emissao_mes,
		e.emissao_dia,
		e.programa_governo AS codigo_programa,
		e.programa_governo_descricao AS programa,
		e.acao_governo AS codigo_acao_ajustada,
		e.acao_governo_descricao AS acao_ajustada,
		e.autor_emendas_orcamento_descricao,
		e.uf_pt AS uf,
		e.uf_pt_descricao AS uf_descricao,
		e.municipio_pt AS municipio,
		'Brasil' AS pais,
		e.ne_ccor,
		e.ne_num_processo,
		e.ne_info_complementar,
		e.ne_ccor_descricao,
		e.doc_observacao,
		e.grupo_despesa AS codigo_gnd,
		e.grupo_despesa_descricao AS gnd,
		e.natureza_despesa,
		e.natureza_despesa_descricao,
		e.modalidade_aplicacao AS codigo_modalidade,
		e.modalidade_aplicacao_descricao AS modalidade,
		e.ne_ccor_favorecido,
		e.ne_ccor_favorecido_descricao,
		e.ne_ccor_ano_emissao,
		e.ptres,
		e.item_informacao,
		e.item_informacao_descricao,
		e.despesas_empenhadas,
		e.despesas_liquidadas,
		e.despesas_pagas,
        e.autor_emendas_orcamento_nome,

		e.emenda_id,

		p.id_parlamentar as id_autor,
		p.cargo_parlamentar as cargo_autor,
		p.nome_parlamentar as autor,
		p.sigla_partido as partido,
		p.uf_parlamentar as uf_autor,
		p.url_foto as url_foto_autor,
		p.email as email_autor,
		p.url_logo_partido as url_foto_partido,

		e.dt_ingest,

		-- Prioridade de cruzamento
		CASE
			WHEN e.emissao_dia >= p.data_filiacao::date 
			 AND e.emissao_dia <= COALESCE(p.data_desfiliacao::date, CURRENT_DATE)
			THEN 1
			-- Se achou nome, mas a data não bateu
			WHEN p.id_parlamentar IS NOT NULL
			THEN 2
			-- Nomes que nem existem
			ELSE 3
		END as prioridade_match,

		-- Distância de fallback para quando não tivermos batido o range
		LEAST(
			ABS(EXTRACT(EPOCH FROM (e.emissao_dia::timestamptz - p.data_filiacao))),
			ABS(EXTRACT(EPOCH FROM (e.emissao_dia::timestamptz - COALESCE(p.data_desfiliacao, CURRENT_TIMESTAMP))))
		) AS distancia_tempo

	FROM tg_emendas_tratado e
	LEFT JOIN parlamentares_hist p
		ON e.chave_join_nome = p.chave_join_nome
),

deduplicado AS (
	SELECT *
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY emenda_id
				ORDER BY 
					prioridade_match ASC,
					distancia_tempo ASC
			) as rn
		FROM cruzamento_bruto
	) sub
	WHERE rn = 1
)

SELECT 
	emissao_mes,
	emissao_dia,
	codigo_programa,
	programa,
	codigo_acao_ajustada,
	acao_ajustada,
	autor_emendas_orcamento_descricao,
    autor_emendas_orcamento_nome,
	uf,
	uf_descricao,
	municipio,
	pais,
	ne_ccor,
	ne_num_processo,
	ne_info_complementar,
	ne_ccor_descricao,
	doc_observacao,
	codigo_gnd,
	gnd,
	natureza_despesa,
	natureza_despesa_descricao,
	codigo_modalidade,
	modalidade,
	ne_ccor_favorecido,
	ne_ccor_favorecido_descricao,
	ne_ccor_ano_emissao,
	ptres,
	item_informacao,
	item_informacao_descricao,
	despesas_empenhadas,
	despesas_liquidadas,
	despesas_pagas,
	
	id_autor,
	cargo_autor,
	autor,
	partido,
	uf_autor,
	url_foto_autor,
	email_autor,
	url_foto_partido,
	
	dt_ingest
FROM deduplicado