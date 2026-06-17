with
    terceirizados_ativos as (
        select *
        from {{ ref("terceirizados") }}
        where situacao = 'Ativo'
    ),

    contratos_ativos as (
        select id, numero, fornecedor_nome, fornecedor_cnpj_cpf_idgener, situacao
        from {{ ref("contratos") }}
        where situacao = 'Ativo'
    )

select
    cc.numero as numero_contrato,
    cc.fornecedor_nome,
    cc.fornecedor_cnpj_cpf_idgener as fornecedor,
    t.*
from terceirizados_ativos as t
inner join contratos_ativos as cc on cast(t.contrato_id as text) = cc.id
