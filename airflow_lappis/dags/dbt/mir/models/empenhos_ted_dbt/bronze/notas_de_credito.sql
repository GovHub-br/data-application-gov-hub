{{ config(materialized="table") }}


with
	notas_de_credito_raw as (
		select
			id_nota::integer as id_nota,
			id_plano_acao::integer as id_plano_acao,
			tx_minuta_nota::text as tx_minuta_nota,
			tx_numero_nota::text as tx_numero_nota,
			dt_emissao_nota::timestamp as dt_emissao_nota,
			cd_gestao_emitente_nota::text as cd_gestao_emitente_nota,
			cd_gestao_favorecida_nota::text as cd_gestao_favorecida_nota,
			tx_situacao_nota::text as tx_situacao_nota,
			cd_ug_emitente_nota::text as cd_ug_emitente_nota,
			cd_ug_favorecida_nota::text as cd_ug_favorecida_nota,
			tx_observacao_nota::text as tx_observacao_nota,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("transfere_gov", "notas_de_credito") }}
	)

select *
from notas_de_credito_raw
