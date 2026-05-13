{{ config(materialized="table") }}

with
	programas_raw as (
		select
			id_programa::integer as id_programa,
			tx_codigo_programa::text as tx_codigo_programa,
			nullif(aa_ano_programa, '')::integer as aa_ano_programa,
			tx_situacao_programa::text as tx_situacao_programa,
			tx_nome_programa::text as tx_nome_programa,
			sigla_unidade_descentralizadora::text as sigla_unidade_descentralizadora,
			unidade_descentralizadora::text as unidade_descentralizadora,
			sigla_unidade_responsavel_acompanhamento::text as sigla_unidade_responsavel_acompanhamento,
			unidade_responsavel_acompanhamento::text as unidade_responsavel_acompanhamento,
			tx_nome_institucional_programa::text as tx_nome_institucional_programa,
			tx_objetivo_programa::text as tx_objetivo_programa,
			tx_descricao_programa::text as tx_descricao_programa,
			nullif(in_grupo_investimento_obra, '')::boolean as in_grupo_investimento_obra,
			nullif(in_grupo_investimento_servico, '')::boolean as in_grupo_investimento_servico,
			nullif(in_grupo_investimento_equipamento, '')::boolean as in_grupo_investimento_equipamento,
			in_autoriza_subdescentralizacao_outro::text as in_autoriza_subdescentralizacao_outro,
			in_autoriza_realizacao_despesas::text as in_autoriza_realizacao_despesas,
			in_autoriza_execucao_creditos_descentralizada::text as in_autoriza_execucao_creditos_descentralizada,
			nullif(in_beneficiario_especifico, '')::boolean as in_beneficiario_especifico,
			nullif(dt_recebimento_plano_beneficiario_inicio, '')::timestamp::date
			as dt_recebimento_plano_beneficiario_inicio,
			nullif(dt_recebimento_plano_beneficiario_fim, '')::timestamp::date
			as dt_recebimento_plano_beneficiario_fim,
			nullif(in_chamamento_publico, '')::boolean as in_chamamento_publico,
			nullif(dt_recebimento_plano_chamamento_inicio, '')::timestamp::date
			as dt_recebimento_plano_chamamento_inicio,
			nullif(dt_recebimento_plano_chamamento_fim, '')::timestamp::date
			as dt_recebimento_plano_chamamento_fim,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("transfere_gov", "programas") }}
	)

select *
from programas_raw
