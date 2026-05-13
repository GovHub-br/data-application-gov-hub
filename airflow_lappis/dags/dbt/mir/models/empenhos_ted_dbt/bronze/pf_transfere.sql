{{ config(materialized="table", alias="pf_transfere_mir") }}


with
	pf_transfere_raw as (
		select
            id_programacao::integer as id_programacao,
			id_plano_acao::integer as id_plano_acao,
			tp_pf_tipo_programacao::text as tp_pf_tipo_programacao,
			tx_minuta_programacao::text as tx_minuta_programacao,
			tx_numero_programacao::text as tx_numero_programacao,
            tx_situacao_programacao::text as tx_situacao_programacao,
			tx_observacao_programacao::text as tx_observacao_programacao,
			ug_emitente_programacao::text as ug_emitente_programacao,
			ug_favorecida_programacao::text as ug_favorecida_programacao,
			dh_recebimento_programacao::timestamp as dh_recebimento_programacao,
			(dt_ingest || '-03:00')::timestamptz as dt_ingest
		from {{ source("transfere_gov", "programacao_financeira") }}
	)

select *
from pf_transfere_raw
