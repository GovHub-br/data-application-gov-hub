with
    lista_servidores as (
        select cpf, dataultimatransacao as dt_ultima_transacao, coduorg as cod_uorg
        from {{ source("siape", "lista_servidores") }}
    )

select *
from lista_servidores
