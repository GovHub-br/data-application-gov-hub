with
    lista_uorgs as (
        select
            cast(codigo as int) as codigo,
            to_date(dataultimatransacao, 'DDMMYYYY') as dt_ultima_transacao,
            nome
        from {{ source("siape", "lista_uorgs") }}
    )

select *
from lista_uorgs
