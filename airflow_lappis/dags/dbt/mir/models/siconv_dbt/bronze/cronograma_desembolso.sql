{{ config(materialized="table") }}

with
    cronograma_desembolso_raw as (
        select
            nullif(id_proposta, '')::integer as id_proposta,
            nullif(nr_convenio, '')::integer as nr_convenio,
            nullif(nr_parcela_crono_desembolso, '')::integer as nr_parcela_crono_desembolso,
            nullif(mes_crono_desembolso, '')::integer as mes_crono_desembolso,
            nullif(ano_crono_desembolso, '')::integer as ano_crono_desembolso,
            tipo_resp_crono_desembolso::text as tipo_resp_crono_desembolso,
            replace(nullif(valor_parcela_crono_desembolso, ''), ',', '.')::numeric(15, 2) as valor_parcela_crono_desembolso
        from {{ source("siconv", "cronograma_desembolso") }}
    )

select *
from cronograma_desembolso_raw