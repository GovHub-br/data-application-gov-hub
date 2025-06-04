with
    dados_financeiros_raw as (
        select
            codrubrica,
            indicadorrd,
            nomerubrica,
            numeroseq,
            valorrubrica,
            dataanomesrubrica,
            pzrubrica,
            mesanopagamento,
            cpf,
            indicadormovsupl,
            perubrica
        from {{ source("siape", "dados_financeiros") }}
    ),

    dados_financeiros_cleaned as (
        select
            nullif(trim(codrubrica), '') as cod_rubrica,
            nullif(trim(indicadorrd), '') as indicador_rd,
            nullif(trim(nomerubrica), '') as nome_rubrica,
            nullif(trim(numeroseq), '') as numero_sequencia,
            nullif(trim(valorrubrica), '') as valor_rubrica_str,  -- Mantém como string
            nullif(trim(dataanomesrubrica), '') as data_anomes_rubrica_str,
            nullif(trim(pzrubrica), '') as prazo_rubrica,
            nullif(trim(mesanopagamento), '') as mes_ano_pagamento_str,
            nullif(trim(cpf), '') as cpf_str,
            nullif(trim(indicadormovsupl), '') as indicador_mov_supl,
            nullif(trim(perubrica), '') as periodo_rubrica
        from dados_financeiros_raw
    ),

    conversao_mes as (
        select
            *,
            case
                upper(substring(data_anomes_rubrica_str, 1, 3))
                when 'JAN'
                then '01'
                when 'FEV'
                then '02'
                when 'MAR'
                then '03'
                when 'ABR'
                then '04'
                when 'MAI'
                then '05'
                when 'JUN'
                then '06'
                when 'JUL'
                then '07'
                when 'AGO'
                then '08'
                when 'SET'
                then '09'
                when 'OUT'
                then '10'
                when 'NOV'
                then '11'
                when 'DEZ'
                then '12'
                else null
            end as mes_num_rubrica,
            substring(data_anomes_rubrica_str, 4, 4) as ano_rubrica,
            case
                upper(substring(mes_ano_pagamento_str, 1, 3))
                when 'JAN'
                then '01'
                when 'FEV'
                then '02'
                when 'MAR'
                then '03'
                when 'ABR'
                then '04'
                when 'MAI'
                then '05'
                when 'JUN'
                then '06'
                when 'JUL'
                then '07'
                when 'AGO'
                then '08'
                when 'SET'
                then '09'
                when 'OUT'
                then '10'
                when 'NOV'
                then '11'
                when 'DEZ'
                then '12'
                else null
            end as mes_num_pagamento,
            substring(mes_ano_pagamento_str, 4, 4) as ano_pagamento
        from dados_financeiros_cleaned
    )

select
    cod_rubrica,
    indicador_rd,
    nome_rubrica,
    numero_sequencia,
    -- Limpa (remove '.') e converte (',' para '.') para NUMERIC
    cast(
        replace(
            replace(valor_rubrica_str, '.', ''),  -- Remove o separador de milhares '.'
            ',',
            '.'  -- Troca a vírgula decimal ',' por '.'
        ) as numeric
    ) as valor_rubrica,
    -- Converte MONYYYY para DATE (primeiro dia do mês)
    to_date(
        ano_rubrica || '-' || mes_num_rubrica || '-01', 'YYYY-MM-DD'
    ) as data_anomes_rubrica,
    prazo_rubrica,
    -- Converte MONYYYY para DATE (primeiro dia do mês)
    to_date(
        ano_pagamento || '-' || mes_num_pagamento || '-01', 'YYYY-MM-DD'
    ) as mes_ano_pagamento,
    regexp_replace(cpf_str, '[^0-9]', '', 'g') as cpf,
    indicador_mov_supl,
    periodo_rubrica
from conversao_mes
