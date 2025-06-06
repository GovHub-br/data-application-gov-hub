with
    fonte as (
        select
            codigounidade,
            nomeunidade,
            siglaunidade,
            municipio,
            uf,
            cargos,
            ordem_grandeza
        from {{ source("siorg", "estrutura_organizacional_cargos") }}
    ),

    cargos_expandidos as (
        select
            f.codigounidade,
            f.nomeunidade,
            f.siglaunidade,
            f.municipio,
            f.uf,
            f.ordem_grandeza,
            cargo_elem
        from
            fonte f,
            lateral jsonb_array_elements(
                replace(replace(f.cargos, '''', '"'), 'None', 'null')::jsonb
            ) as cargo_elem
    ),

    instancias_expandidas as (
        select
            ce.codigounidade,
            ce.nomeunidade,
            ce.siglaunidade,
            ce.municipio,
            ce.uf,
            ce.ordem_grandeza,
            cargo_elem ->> 'denominacao' as denominacao,
            cargo_elem ->> 'funcao' as funcao,
            instancia_elem ->> 'codigoInstancia' as codigo_instancia,
            instancia_elem ->> 'nomeTitular' as nome_titular,
            instancia_elem ->> 'cpfTitular' as cpf_titular
        from
            cargos_expandidos ce,
            lateral jsonb_array_elements(cargo_elem -> 'instancias') as instancia_elem
    ),

    instancias_filtradas as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by codigo_instancia order by ordem_grandeza desc
                    ) as rn
                from instancias_expandidas
            ) t
        where rn = 1
    )

select
    codigounidade,
    nomeunidade,
    siglaunidade,
    municipio,
    uf,
    denominacao,
    funcao,
    codigo_instancia,
    nome_titular,
    cpf_titular,
    ordem_grandeza
from instancias_filtradas
