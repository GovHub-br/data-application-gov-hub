with
    lista_servidores as (
        select 
            cpf, 
            dataultimatransacao as dt_ultima_transacao,
            coduorg as cod_uorg,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("siape", "lista_servidores") }}
    )

select
    *,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from lista_servidores
