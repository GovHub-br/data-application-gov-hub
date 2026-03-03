with dados_funcionais_extract as (
    select
        sigla_nivel_cargo as nivel,
        cod_classe || '-' || cod_padrao as classe_padrao,
        nome_situacao_funcional,
        nome_cargo,
        count(1) as qtd,
        max(dt_ingest) as dt_ingest
    from {{ ref('dados_funcionais') }}
    where nome_situacao_funcional != 'APOSENTADO'
    group by 1, 2, 3, 4
)

select *
from dados_funcionais_extract
