with
    contratos_com_ne_cnpj_cpf as (
        select distinct contrato_id, ne, cnpj_cpf
        from {{ ref("identificadores") }}
        where ne is not null
    ),

    empenhos_tesouro_transformed as (
        select
            *,
            case
                when ne_ccor is not null then upper(right(ne_ccor, 12))
            end as ne_transformed
        from {{ ref("empenhos_tesouro") }}
    ),

    -- Primeiro merge: apenas os contratos que tem ne e cnpj_cpf
    full_join as (
        select
            *,
            case
                when c.cnpj_cpf is not null and e.ne_ccor_favorecido is not null
                then 'both'
                when c.cnpj_cpf is not null and e.ne_ccor_favorecido is null
                then 'left only'  -- nao existe no empenhos_tesouro
                when c.cnpj_cpf is null and e.ne_ccor_favorecido is not null
                then 'right only'  -- existe no empenhos_tesouro mas nao no raw.contratos (ESSA QUE NÓS QUEREMOS)
            end as origem
        from contratos_com_ne_cnpj_cpf as c
        full join
            empenhos_tesouro_transformed as e
            on c.ne = e.ne_transformed
            and c.cnpj_cpf = e.ne_ccor_favorecido
    ),

    resultado_1 as (
        select
            contrato_id,
            coalesce(ne_transformed, ne) as ne_transformed,
            ne_ccor,
            ne_info_complementar,
            ne_num_processo,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_ano_emissao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            dt_ingest
        from full_join
        where origem = 'both' or origem = 'left only'
    -- contrato_id nulo significa lacuna no lado esquerdo do RIGHT JOIN, 
    -- ou contratos em que o join usando ne e cnpj/cpf não foi possível
    ),

    -- ---------------------------------------------------------------------------------------------------
    empenhos_restantes_1 as (
        select *
        from empenhos_tesouro_transformed
        where ne_ccor in (select ne_ccor from full_join where origem = 'right only')
    ),

    todos_contratos as (
        select distinct contrato_id, processo, cnpj_cpf, info_complementar
        from {{ ref("identificadores") }}
    ),

    -- Segundo merge: usando processos em que há um único contrato
    processos_unicos as (
        select distinct contrato_id, processo as num_processo
        from todos_contratos
        where
            processo in (
                select processo from todos_contratos group by processo having count(*) = 1
            )
    ),

    juncao_processo as (
        select *
        from processos_unicos as cpu
        right join empenhos_restantes_1 as er on cpu.num_processo = er.ne_num_processo
    ),

    resultado_2 as (
        select
            contrato_id,
            ne_transformed,
            ne_ccor,
            ne_info_complementar,
            ne_num_processo,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_ano_emissao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            dt_ingest
        from juncao_processo
        -- WHERE origem = 'both'
        where contrato_id is not null
    ),
    -- ---------------------------------------------------------------------------------------------------
    empenhos_restantes_2 as (
        select *
        from empenhos_restantes_1
        where ne_ccor not in (select ne_ccor from resultado_2)
    ),

    -- Terceiro merge: usando CNPJs que possuem um único contrato
    cnpjs_unicos as (
        select distinct contrato_id, cnpj_cpf
        from todos_contratos
        where
            cnpj_cpf in (
                select cnpj_cpf from todos_contratos group by cnpj_cpf having count(*) = 1
            )
    ),

    juncao_cnpjs as (
        select *
        from cnpjs_unicos as cu
        right join empenhos_restantes_2 as er on cu.cnpj_cpf = er.ne_ccor_favorecido
    ),

    resultado_3 as (
        select
            contrato_id,
            ne_transformed,
            ne_ccor,
            ne_info_complementar,
            ne_num_processo,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_ano_emissao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            dt_ingest
        from juncao_cnpjs
        where contrato_id is not null
    ),
    -- ---------------------------------------------------------------------------------------------------
    empenhos_restantes_3 as (
        select
            *,
            -- garantir que ambos os lados estão no mesmo formato
            substring(ne_info_complementar from '^([0-9]+) -') as info_complementar
        from empenhos_restantes_2
        where ne_ccor not in (select ne_ccor from resultado_3)
    ),

    contratos_info_complementar as (
        select distinct contrato_id, info_complementar from todos_contratos
    ),

    -- Quarto merge: usando "informação complementar", que é um agregado da unidade
    -- gestora + modalidade + numero do contrato ou numero da licitação
    juncao_info_complementar as (
        select *
        from contratos_info_complementar as c
        right join empenhos_restantes_3 as e on c.info_complementar = e.info_complementar
    ),

    resultado_4 as (
        select
            contrato_id,
            ne_transformed,
            ne_ccor,
            ne_info_complementar,
            ne_num_processo,
            ne_ccor_descricao,
            doc_observacao,
            natureza_despesa,
            natureza_despesa_descricao,
            ne_ccor_favorecido,
            ne_ccor_ano_emissao,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            dt_ingest
        from juncao_info_complementar
        where contrato_id is not null
    ),

    -- União de todos os resultados parciais
    resultado_final as (
        select *
        from resultado_1
        union
        select *
        from resultado_2
        union
        select *
        from resultado_3
        union
        select *
        from resultado_4
    )

select *
from resultado_final
