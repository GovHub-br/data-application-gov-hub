with
    fonte as (
        select
            codigotipo,
            nome,
            sigla,
            codigocargofuncao,
            categoria,
            nivel,
            regraautoridade,
            atonormativo__tipoato,
            atonormativo__codigounidade,
            atonormativo__numero,
            atonormativo__dataassinatura,
            atonormativo__datapublicacao,
            atonormativo__datavigencia,
            atonormativo__ementa,
            atonormativo__url,
            atonormativo__codigotipo,
            atonormativo__siglatipo,
            denominacoes__denominacao
        from {{ source("siorg", "cargos_funcao") }}
    ),

    denominacoes_expandidas as (
        select
            f.*,
            denominacao_elem ->> 'codigo' as denominacao_codigo,
            denominacao_elem ->> 'descricao' as denominacao_descricao
        from
            fonte f,
            lateral jsonb_array_elements(
                replace(
                    replace(f.denominacoes__denominacao, '''', '"'), 'None', 'null'
                )::jsonb
            ) as denominacao_elem
    )

select
    codigotipo,
    nome,
    sigla,
    codigocargofuncao,
    categoria,
    nivel,
    regraautoridade,
    atonormativo__tipoato,
    atonormativo__codigounidade,
    atonormativo__numero,
    atonormativo__dataassinatura,
    atonormativo__datapublicacao,
    atonormativo__datavigencia,
    atonormativo__ementa,
    atonormativo__url,
    atonormativo__codigotipo,
    atonormativo__siglatipo,
    denominacao_codigo,
    denominacao_descricao
from denominacoes_expandidas
