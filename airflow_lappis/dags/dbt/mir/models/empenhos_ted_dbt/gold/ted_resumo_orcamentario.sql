{{ config(materialized="table") }}

with
	planos_acao_deduplicado as (
		select
			id_plano_acao,
			id_programa,
			sq_instrumento as num_transf,
			sigla_unidade_descentralizada,
			vl_total_plano_acao,
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
	),

	programas_tb as (
		select
			pad.id_plano_acao,
			prog.sigla_unidade_responsavel_acompanhamento,
			prog.tx_nome_institucional_programa,
			prog.tx_objetivo_programa
		from planos_acao_deduplicado pad
		left join {{ ref("programas_ted") }} prog using (id_programa)
	),

	valor_firmado_tb as (
		select
			id_plano_acao as plano_acao,
			num_transf,
			vl_total_plano_acao as valor_firmado,
			sigla_unidade_descentralizada,
			dt_ingest_plano_acao as dt_ingest_vf
		from planos_acao_deduplicado
	),

	valores_orcamentos_tb as (
		select
			id_plano_acao as plano_acao,
			nc_transferencia as num_transf,
			sum(
				case
					when nc_evento in ('300301', '300307') then 0
					else valor_celula
				end
			) as orcamento_recebido,
			sum(
				case
					when nc_evento in ('300301', '300307') then valor_celula
					else 0
				end
			) as orcamento_devolvido,
			max(programa_governo) as programa_governo,
			max(programa_governo_descricao) as programa_governo_descricao,
			max(dt_ingest) as dt_ingest_vo
		from {{ ref("nc_plano_acao") }}
		where ptres not in ('-9')
		group by id_plano_acao, nc_transferencia
	),

	valores_empenhados_tb as (
		select
			plano_acao,
			num_transf,
			sum(
				case when despesas_empenhadas > 0 then despesas_empenhadas else 0 end
			) as empenhado,
			sum(
				case when despesas_empenhadas < 0 then -despesas_empenhadas else 0 end
			) as empenho_anulado,
			sum(despesas_pagas) as despesas_pagas_exercicio,
			sum(restos_a_pagar_pagos) as despesas_pagas_rap,
			sum(restos_a_pagar_inscritos) as restos_a_pagar,
			sum(despesas_liquidadas) as despesas_liquidada,
			max(dt_ingest) as dt_ingest_ve
		from {{ ref("empenhos_por_plano_acao") }}
		group by plano_acao, num_transf
	),

	valores_financeiro_tb as (
		select
			id_plano_acao as plano_acao,
			pf_inscricao as num_transf,
			sum(
				case
					when substring(pf_acao_descricao, '(\w+) ') = 'TRANSFERENCIA'
					then pf_valor_linha
					else 0
				end
			) as financeiro_recebido,
			sum(
				case
					when substring(pf_acao_descricao, '(\w+) ') = 'DEVOLUCAO'
					then pf_valor_linha
					else 0
				end
			) as financeiro_devolvido,
			sum(
				case
					when substring(pf_acao_descricao, '(\w+) ') = 'CANCELAMENTO'
					then pf_valor_linha
					else 0
				end
			) as financeiro_cancelado,
			max(dt_ingest) as dt_ingest_vfin
		from {{ ref("pf_unificado") }}
		group by id_plano_acao, pf_inscricao
	),

	join_parcial as (
		select
			*,
			greatest(vo.dt_ingest_vo, ve.dt_ingest_ve, vfin.dt_ingest_vfin) as dt_ingest_jp
		from valores_orcamentos_tb vo
		full join valores_empenhados_tb ve using (plano_acao, num_transf)
		full join valores_financeiro_tb vfin using (plano_acao, num_transf)
	)

select
	plano_acao,
	num_transf,
	sigla_unidade_descentralizada,
	valor_firmado,
	orcamento_recebido,
	orcamento_devolvido,
	empenhado,
	empenho_anulado,
	despesas_pagas_exercicio,
	despesas_pagas_rap,
	restos_a_pagar,
	despesas_liquidada,
	financeiro_recebido,
	financeiro_devolvido,
	financeiro_cancelado,
	greatest(vf.dt_ingest_vf, jp.dt_ingest_jp) as dt_ingest,
	prog.sigla_unidade_responsavel_acompanhamento,
	prog.tx_nome_institucional_programa,
	prog.tx_objetivo_programa,
	jp.programa_governo,
	jp.programa_governo_descricao
from valor_firmado_tb vf
full join join_parcial jp using (plano_acao, num_transf)
left join programas_tb prog on plano_acao = prog.id_plano_acao
where (plano_acao is not null) or (num_transf is not null)
