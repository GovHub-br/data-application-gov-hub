{{ config(materialized="table") }}

with
	raw_data as (
		select *
		from {{ ref("nc_unificado") }}
		where nc_transferencia != '-8'
	),

	planos_de_acao as (
		select distinct *
		from {{ ref("num_transf_n_plano_acao") }}
		where plano_acao is not null
	),

	result_table as (
		select
			rd.*,
			pda.plano_acao::integer as id_plano_acao
		from raw_data rd
		left join planos_de_acao pda on rd.nc_transferencia = pda.num_transf
	)

select *
from result_table
