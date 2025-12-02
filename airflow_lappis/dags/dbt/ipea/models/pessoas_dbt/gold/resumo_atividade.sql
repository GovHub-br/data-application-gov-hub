with

    dados_funcionais_extract as (
        select
            nome_situacao_funcional,
            nome_cargo,
            sigla_nivel_cargo as nivel,
            cod_classe || '-' || cod_padrao as classe_padrao,
            sum(1) as qtd
        from {{ ref("dados_funcionais") }}
        where nome_situacao_funcional != 'APOSENTADO'
        group by nome_situacao_funcional, nome_cargo, nivel, classe_padrao
    )

select *
from dados_funcionais_extract
