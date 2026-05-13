{{ config(materialized="table") }}

with
	pf_unificado as (
		select *
		from {{ ref("pf_unificado") }}
	),
	planos_acao_deduplicado as (
		select
			id_plano_acao,
			id_programa,
			sigla_unidade_descentralizada,
			unidade_descentralizada,
			sigla_unidade_responsavel_execucao,
			unidade_responsavel_execucao,
			vl_total_plano_acao,
			dt_inicio_vigencia,
			dt_fim_vigencia,
			tx_objeto_plano_acao,
			tx_justificativa_plano_acao,
			in_forma_execucao_direta,
			in_forma_execucao_particulares,
			in_forma_execucao_descentralizada,
			tx_situacao_plano_acao,
			aa_ano_plano_acao,
			vl_beneficiario_especifico,
			vl_chamamento_publico,
			sq_instrumento,
			aa_instrumento,
			dt_ingest as dt_ingest_plano_acao
		from (
			select
				pa.*,
				row_number() over (
					partition by pa.id_plano_acao
					order by pa.dt_ingest desc
				) as rn
			from {{ ref("planos_acao_ted") }} pa
		) pa_filtrado
		where rn = 1
	)

select
	pf.emissao_mes,
	pf.emissao_dia,
	pf.ug_emitente,
	pf.ug_emitente_descricao,
	pf.ug_favorecido,
	pf.ug_favorecido_descricao,
	pf.pf_evento,
	pf.pf_evento_descricao,
	pf.pf,
	pf.pf_inscricao,
	pf.pf_acao,
	pf.pf_acao_descricao,
	pf.pf_fonte_recursos,
	pf.pf_fonte_recursos_descricao,
	pf.doc_observacao,
	pf.pf_valor_linha,
	pf.id_programacao,
	pf.id_plano_acao,
	pf.tp_pf_tipo_programacao,
	pf.tx_minuta_programacao,
	pf.tx_numero_programacao,
	pf.tx_situacao_programacao,
	pf.tx_observacao_programacao,
	pf.ug_emitente_programacao,
	pf.ug_favorecida_programacao,
	pf.dh_recebimento_programacao,
	pf.pf_chave,
	pa.id_programa,
	pa.sigla_unidade_descentralizada,
	pa.unidade_descentralizada,
	pa.sigla_unidade_responsavel_execucao,
	pa.unidade_responsavel_execucao,
	pa.vl_total_plano_acao,
	pa.dt_inicio_vigencia,
	pa.dt_fim_vigencia,
	pa.tx_objeto_plano_acao,
	pa.tx_justificativa_plano_acao,
	pa.in_forma_execucao_direta,
	pa.in_forma_execucao_particulares,
	pa.in_forma_execucao_descentralizada,
	pa.tx_situacao_plano_acao,
	pa.aa_ano_plano_acao,
	pa.vl_beneficiario_especifico,
	pa.vl_chamamento_publico,
	pa.sq_instrumento,
	pa.aa_instrumento,
	coalesce(
		greatest(pf.dt_ingest, pa.dt_ingest_plano_acao),
		pf.dt_ingest,
		pa.dt_ingest_plano_acao
	) as dt_ingest
from pf_unificado pf
left join planos_acao_deduplicado pa using (id_plano_acao)
