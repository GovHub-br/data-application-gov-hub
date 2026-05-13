{{ config(materialized="table") }}

with
    emendas as (
        select *,
            case 
                when ne_info_complementar ~ '^\d+$' 
                    then ne_info_complementar::integer
                when ne_ccor_descricao ~* 'CONVENIO|FOMENTO|FOMENO'
                    then nullif(
                        regexp_replace(
                            ne_ccor_descricao, 
                            '.*(?:CONVENIO|FOMENTO|FOMENO)\s*(?:N[°º]?)?\s*(\d{6}).*', 
                            '\1'
                        ), 
                        ne_ccor_descricao
                    )::integer
                when ne_ccor_descricao ~* 'TED\s*\d{6}'
                    then nullif(
                        regexp_replace(
                            ne_ccor_descricao, 
                            '.*TED\s*(\d{6}).*', 
                            '\1'
                        ), 
                        ne_ccor_descricao
                    )::integer
                when doc_observacao ~* 'CONVENIO|FOMENTO|FOMENO'
                    then nullif(
                        regexp_replace(
                            doc_observacao, 
                            '.*(?:CONVENIO|FOMENTO|FOMENO)\s*(?:N[°º]?)?\s*(\d{6}).*', 
                            '\1'
                        ), 
                        doc_observacao
                    )::integer
                when doc_observacao ~* 'TED\s*\d{6}'
                    then nullif(
                        regexp_replace(
                            doc_observacao, 
                            '.*TED\s*(\d{6}).*', 
                            '\1'
                        ), 
                        doc_observacao
                    )::integer
                else null 
            end as numero_transferencia
        from {{ ref("emendas_partidos") }}
    )

select *
from emendas