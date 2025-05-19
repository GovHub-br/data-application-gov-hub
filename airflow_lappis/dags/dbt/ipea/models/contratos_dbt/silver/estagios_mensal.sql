with

    parsed_estagios as (
        select
            right(ne_ccor, 12) as ne,
            case when emissao_dia like '000/%' then true else false end as eh_rap,
            case
                when emissao_dia like '000/%'
                then '01'
                else substring(emissao_dia, '\/(\d{2})\/')
            end as mes_lancamento,
            right(emissao_mes, 4) as ano_lancamento,
            ne_ccor_favorecido as cnpj_cpf,
            substring(ne_info_complementar, '(^[0-9]+)') as info_complementar,
            ne_num_processo,
            despesas_empenhadas as valor_empenhado,
            despesas_liquidadas as valor_liquidado,
            despesas_pagas as valor_pago,
            restos_a_pagar_inscritos as restos_a_pagar,
            restos_a_pagar_pagos as restos_a_pagar_pago
        from {{ ref("empenhos_tesouro") }}
        where true and ne_ccor != 'Total'
    ),

    grouped_estagios as (
        select
            ne,
            eh_rap,
            ano_lancamento::integer as ano_lancamento,
            mes_lancamento,
            cnpj_cpf,
            max(info_complementar) as info_complementar,
            max(ne_num_processo) as num_processo,
            sum(valor_empenhado) as valor_empenhado,
            sum(valor_liquidado) as valor_liquidado,
            sum(valor_pago) as valor_pago,
            sum(restos_a_pagar) as restos_a_pagar,
            sum(restos_a_pagar_pago) as restos_a_pagar_pago
        from parsed_estagios
        group by 1, 2, 3, 4, 5
        order by 1, 2
    ),

    processo_fixed as (
        select
            ne,
            cnpj_cpf,
            info_complementar,
            eh_rap,
            mes_lancamento,
            ano_lancamento,
            case
                when eh_rap
                then array[ano_lancamento - 1, ano_lancamento]
                else array[ano_lancamento]
            end as ano_efetivo,
            min(num_processo) over (partition by ne) as num_processo,
            valor_empenhado,
            valor_liquidado,
            valor_pago,
            restos_a_pagar,
            restos_a_pagar_pago
        from grouped_estagios
    ),

    unnest_rap as (
        select
            ne,
            cnpj_cpf,
            info_complementar,
            eh_rap,
            mes_lancamento,
            ano_lancamento,
            unnest(ano_efetivo) as ano_efetivo,
            num_processo,
            valor_empenhado,
            valor_liquidado,
            valor_pago,
            restos_a_pagar,
            restos_a_pagar_pago
        from processo_fixed
    ),

    fix_data as (
        select
            ne,
            cnpj_cpf,
            info_complementar,
            eh_rap,
            case
                when ano_efetivo = ano_lancamento then mes_lancamento else '12'
            end as mes_lancamento,
            ano_lancamento,
            ano_efetivo,
            num_processo,
            valor_empenhado,
            valor_liquidado,
            valor_pago,
            case
                when ano_efetivo > ano_lancamento
                then restos_a_pagar
                else - restos_a_pagar
            end as restos_a_pagar,
            restos_a_pagar_pago
        from unnest_rap
    ),

    results as (
        select
            ne,
            cnpj_cpf,
            info_complementar,
            num_processo,
            to_date(
                ano_efetivo || '-' || mes_lancamento || '-01', 'YYYY-MM-DD'
            ) as mes_lancamento,
            valor_empenhado,
            valor_liquidado,
            valor_pago,
            restos_a_pagar,
            restos_a_pagar_pago
        from fix_data
    )

--
select *
from results
order by ne, cnpj_cpf, mes_lancamento
