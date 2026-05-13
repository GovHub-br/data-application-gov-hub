{{ config(materialized="table") }}

with
	pf_tesouro as (
		select
			emissao_mes,
			emissao_dia,
			ug_emitente,
			ug_emitente_descricao,
			ug_favorecido,
			ug_favorecido_descricao,
			pf_evento,
			pf_evento_descricao,
			pf,
			pf_inscricao,
			pf_acao,
			pf_acao_descricao,
			pf_fonte_recursos,
			pf_fonte_recursos_descricao,
			doc_observacao,
			pf_valor_linha,
			dt_ingest as dt_ingest_tesouro,
			upper(trim(right(pf, 12))) as pf_chave
		from {{ ref("pf_tesouro") }}
	),
	pf_transfere as (
		select
			id_programacao,
			id_plano_acao,
			tp_pf_tipo_programacao,
			tx_minuta_programacao,
			tx_numero_programacao,
			tx_situacao_programacao,
			tx_observacao_programacao,
			ug_emitente_programacao,
			ug_favorecida_programacao,
			dh_recebimento_programacao,
			dt_ingest as dt_ingest_transfere,
			upper(trim(tx_numero_programacao)) as pf_chave
		from {{ ref("pf_transfere") }}
	)

select
	t.emissao_mes,
	t.emissao_dia,
	t.ug_emitente,
	t.ug_emitente_descricao,
	t.ug_favorecido,
	t.ug_favorecido_descricao,
	t.pf_evento,
	t.pf_evento_descricao,
	t.pf,
	t.pf_inscricao,
	t.pf_acao,
	t.pf_acao_descricao,
	t.pf_fonte_recursos,
	t.pf_fonte_recursos_descricao,
	t.doc_observacao,
	t.pf_valor_linha,
	p.id_programacao,
	p.id_plano_acao,
	p.tp_pf_tipo_programacao,
	p.tx_minuta_programacao,
	p.tx_numero_programacao,
	p.tx_situacao_programacao,
	p.tx_observacao_programacao,
	p.ug_emitente_programacao,
	p.ug_favorecida_programacao,
	p.dh_recebimento_programacao,
	t.pf_chave,
	greatest(t.dt_ingest_tesouro, p.dt_ingest_transfere) as dt_ingest
from pf_tesouro t
inner join pf_transfere p using (pf_chave)
