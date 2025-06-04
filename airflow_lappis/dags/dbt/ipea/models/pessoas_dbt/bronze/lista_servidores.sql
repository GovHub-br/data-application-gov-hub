<< << << < head
with
    lista_servidores as (
        select
            cpf,
            to_date(dataultimatransacao, 'DDMMYYYY') as dt_ultima_transacao,
            coduorg as cod_uorg
        from {{ source("siape", "lista_servidores") }}
    )
    == ==
    == =
with
    lista_servidores as (
        select cpf, dt_ultima_transacao, cod_uorg
        from {{ source("siape", "lista_servidores") }}
    )
    >> >>
    >> > 2884731 (changed tenant siape dbt to pessoas dbt)

select *
from lista_servidores
