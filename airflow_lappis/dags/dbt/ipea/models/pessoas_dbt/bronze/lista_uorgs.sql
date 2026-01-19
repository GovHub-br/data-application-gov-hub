with
    lista_uorgs as (
        select cast(codigo as int) as codigo, dt_ultima_transacao, nome, (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("siape", "lista_uorgs") }}
    )

select *
from lista_uorgs
