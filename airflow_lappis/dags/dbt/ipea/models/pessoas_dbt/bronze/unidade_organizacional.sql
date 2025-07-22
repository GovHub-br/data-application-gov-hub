with recursive
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
            datafinalversaoconsulta,
            operacao,
            codigounidadepaianterior,
            codigoorgaoentidadeanterior
        from {{ source("siorg", "unidade_organizacional") }}
    ),

    unidades_raiz as (select '7' as codigounidade_raiz),

    hierarquia as (
        select f.*, 1 as ordem_grandeza, sigla as caminho_unidade
        from fonte f
        join unidades_raiz r on f.codigounidade = r.codigounidade_raiz

        union all

        select
            f.*,
            h.ordem_grandeza + 1 as ordem_grandeza,
            h.caminho_unidade || '-' || lpad(f.sigla::text, 5, '0') as caminho_unidade
        from fonte f
        join hierarquia h on f.codigounidadepai = h.codigounidade
    )

select *
from hierarquia
