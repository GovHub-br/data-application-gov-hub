{{ config(materialized="table") }}

with
    projetos as (
        select
            {{ safe_bigint('projetoid') }} as projetoid,
            {{ safe_bigint('categoriaadicionalprojetoid') }} as categoriaadicionalprojetoid,
            tituloprojeto::text as tituloprojeto,
            objetivofinalprojeto::text as objetivofinalprojeto,
            justificativaprojeto::text as justificativaprojeto,
            metodologiaprojeto::text as metodologiaprojeto,
            cooperacao::text as cooperacao,
            {{ safe_date('datainicio') }} as datainicio,
            {{ safe_date('datafim') }} as datafim,
            {{ safe_bigint('coordenadorid') }} as coordenadorid,
            {{ safe_bigint('diretoriaid') }} as diretoriaid,
            {{ safe_bigint('coordenacaoid') }} as coordenacaoid,
            {{ safe_bigint('statusetapaid') }} as statusetapaid,
            {{ safe_bigint('statusprojetoid') }} as statusprojetoid,
            {{ safe_bigint('modalidadeprojetoid') }} as modalidadeprojetoid,
            numeroprojeto::text as numeroprojeto,
            numeroprojetonoano::text as numeroprojetonoano,
            {{ safe_bigint('anoprojeto') }} as anoprojeto,
            {{ safe_bigint('usuariocadastranteid') }} as usuariocadastranteid,
            {{ safe_bigint('usuarioversaoid') }} as usuarioversaoid,
            {{ safe_bigint('numeroversao') }} as numeroversao,
            {{ safe_timestamp('horaversao') }} as horaversao,
            {{ safe_boolean('projetoexcluido') }} as projetoexcluido,
            {{ safe_bigint('usuarioexcluinteid') }} as usuarioexcluinteid,
            {{ safe_bigint('rowversion') }} as rowversion,
            {{ safe_boolean('projetoassessoria') }} as projetoassessoria,
            {{ safe_boolean('projetoestrategico') }} as projetoestrategico,
            {{ safe_date('datafimreal') }} as datafimreal,
            {{ safe_bigint('statusprojetohistoricoid') }} as statusprojetohistoricoid,
            palavras_chave::text as palavras_chave,
            planocomunicacao::text as planocomunicacao,
            justificativaprojetoestrategico::text as justificativaprojetoestrategico,
            {{ safe_boolean('planejamentofinanceiroativo') }} as planejamentofinanceiroativo,
            {{ safe_boolean('projetoprioritario') }} as projetoprioritario
        from {{ source("ipea_pro", "projetos") }}
    )

select *
from projetos
