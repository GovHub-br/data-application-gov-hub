with
    fonte as (
        select
            regexp_replace(codigounidade, '^.*/', '') as codigounidade,
            regexp_replace(codigounidadepai, '^.*/', '') as codigounidadepai,
            regexp_replace(codigoorgaoentidade, '^.*/', '') as codigoorgaoentidade,
            regexp_replace(codigotipounidade, '^.*/', '') as codigotipounidade,
            nome,
            sigla,
            regexp_replace(codigoesfera, '^.*/', '') as codigoesfera,
            regexp_replace(codigopoder, '^.*/', '') as codigopoder,
            regexp_replace(codigonaturezajuridica, '^.*/', '') as codigonaturezajuridica,
            codigosubnaturezajuridica,
            nivelnormatizacao,
            versaoconsulta,
            datainicialversaoconsulta,
            datafinalversaoconsulta,
            operacao,
            codigounidadepaianterior,
            codigoorgaoentidadeanterior
        from {{ source("siorg", "unidade_organizacional") }}
    )

select *
from fonte
