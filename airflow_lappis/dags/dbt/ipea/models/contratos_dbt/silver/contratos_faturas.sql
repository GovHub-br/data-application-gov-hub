{{ config(materialized="table") }}

with
    contratos as (
        select
            id::int as contrato_id,
            fornecedor_cnpj_cpf_idgener,
            processo as processo_contrato,
            numero as numero_contrato,
            objeto as objeto_contrato
        from {{ ref("contratos") }}
    ),

    faturas_base as (select * from {{ ref("faturas") }})

select
    f.id,
    f.contrato_id,
    c.numero_contrato,
    c.processo_contrato as contrato_processo,
    c.fornecedor_cnpj_cpf_idgener,
    c.objeto_contrato,
    f.tipolistafatura_id,
    f.justificativafatura_id,
    f.sfadrao_id,
    f.numero,
    f.emissao,
    f.prazo,
    f.vencimento,
    f.valor,
    f.juros,
    f.multa,
    f.glosa,
    f.valorliquido,
    f.processo,
    f.protocolo,
    f.ateste,
    f.repactuacao,
    f.infcomplementar,
    f.mesref,
    f.anoref,
    f.situacao,
    f.chave_nfe,
    f.dados_referencia,
    f.dados_item_faturado,
    f.dados_empenho,
    f.id_empenho,
    f.numero_empenho,
    f.valor_empenho,
    f.subelemento
from faturas_base f
left join contratos c on f.contrato_id = c.contrato_id
