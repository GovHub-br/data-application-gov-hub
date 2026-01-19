with

    valores_pagos_contratos as (
        select contrato_id as id, sum(despesas_pagas) as despesas_pagas, max(dt_ingest) as dt_ingest_vpc
        from {{ ref("contratos_empenhos") }}
        where contrato_id is not null
        group by contrato_id
    ),

    contratos_gold as (
        select
            *,
            case
                when vp.despesas_pagas = c.valor_global then 'Sim' else 'Não'
            end as pendente_baixa
        from {{ ref("contratos") }} as c
        left join valores_pagos_contratos as vp using (id)
    )

--
select
    id as contrato_id,
    fornecedor_cnpj_cpf_idgener as fornecedor_cnpj_cpf,
    numero,
    categoria,
    modalidade,
    tipo,
    situacao,
    pendente_baixa,
    fornecedor_nome,
    objeto,
    valor_global,
    despesas_pagas,
    vigencia_inicio,
    vigencia_fim,
    num_parcelas,
    case
        when fornecedor_tipo = 'IDGENERICO'
        then 'Empresa do Exterior'
        else fornecedor_tipo
    end as fornecedor_tipo,
    concat(
        contratante__orgao_origem__unidade_gestora_origem__codigo,
        ' - ',
        contratante__orgao_origem__unidade_gestora_origem__nome_resumido
    ) as "Unidade",
    case
        when vigencia_fim - vigencia_inicio >= 730 and num_parcelas > 1
        then 'Sim'
        else 'Não'
    end as continuado,
    greatest(dt_ingest, dt_ingest_vpc) as dt_ingest
from contratos_gold
