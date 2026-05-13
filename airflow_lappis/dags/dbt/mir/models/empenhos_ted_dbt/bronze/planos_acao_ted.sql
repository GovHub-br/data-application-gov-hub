{{ config(materialized="table") }}

with
	planos_acao_raw as (
		select
			id_plano_acao::integer as id_plano_acao,
			id_programa::integer as id_programa,
			sigla_unidade_descentralizada::text as sigla_unidade_descentralizada,
			unidade_descentralizada::text as unidade_descentralizada,
			sigla_unidade_responsavel_execucao::text as sigla_unidade_responsavel_execucao,
			unidade_responsavel_execucao::text as unidade_responsavel_execucao,
			nullif(vl_total_plano_acao, '')::numeric(15, 2) as vl_total_plano_acao,
			nullif(dt_inicio_vigencia, '')::timestamp::date as dt_inicio_vigencia,
			nullif(dt_fim_vigencia, '')::timestamp::date as dt_fim_vigencia,
			tx_objeto_plano_acao::text as tx_objeto_plano_acao,
			tx_justificativa_plano_acao::text as tx_justificativa_plano_acao,
			nullif(in_forma_execucao_direta, '')::boolean as in_forma_execucao_direta,
			nullif(in_forma_execucao_particulares, '')::boolean as in_forma_execucao_particulares,
			nullif(in_forma_execucao_descentralizada, '')::boolean as in_forma_execucao_descentralizada,
			tx_situacao_plano_acao::text as tx_situacao_plano_acao,
			nullif(aa_ano_plano_acao, '')::integer as aa_ano_plano_acao,
			nullif(vl_beneficiario_especifico, '')::numeric(15, 2) as vl_beneficiario_especifico,
			nullif(vl_chamamento_publico, '')::numeric(15, 2) as vl_chamamento_publico,
			sq_instrumento::text as sq_instrumento,
			nullif(aa_instrumento, '')::integer as aa_instrumento,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("transfere_gov", "planos_acao") }}
	)

select *
from planos_acao_raw
