with
    lista_servidores as (
        select cpf, dt_ultima_transacao, cod_uorg
        from {{ source("siape", "lista_servidores") }}
    )

select *
from lista_servidores
