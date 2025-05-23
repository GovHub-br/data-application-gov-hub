with
    fonte as (
        select codigounidade, nomeunidade, siglaunidade, municipio, uf, cargos
        from {{ source("siorg", "estrutura_organizacional_cargos") }}
    ),

    cargos_expandidos as (
        select
            f.codigounidade, f.nomeunidade, f.siglaunidade, f.municipio, f.uf, cargo_elem
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
            cargo_elem ->> 'denominacao' as denominacao,
            cargo_elem ->> 'funcao' as funcao,
            instancia_elem ->> 'codigoInstancia' as codigo_instancia,
            instancia_elem ->> 'nomeTitular' as nome_titular,
            instancia_elem ->> 'cpfTitular' as cpf_titular
        from
            cargos_expandidos ce,
            lateral jsonb_array_elements(cargo_elem -> 'instancias') as instancia_elem
    )

select
    codigounidade,
    nomeunidade,
    siglaunidade,
    municipio,
    uf,
    denominacao,
    funcao,
    codigo_instancia::bigint as codigo_instancia,
    nome_titular,
    cpf_titular
from instancias_expandidas
