{{ config(materialized="table") }}

with
    servidorespublicos as (
        select
            {{ safe_bigint('servidorpublicoid') }} as servidorpublicoid,
            {{ safe_bigint('categoriaadicionalservidorid') }} as categoriaadicionalservidorid,
            nomeservidor::text as nomeservidor,
            {{ safe_date('datanascimento') }} as datanascimento,
            regexp_replace(numerocpf, '[^0-9]', '', 'g')::text as numerocpf,
            numerorg::text as numerorg,
            expedidorrg::text as expedidorrg,
            {{ safe_bigint('diretoriaid') }} as diretoriaid,
            {{ safe_bigint('coordenacaoid') }} as coordenacaoid,
            {{ safe_numeric('salarioatual') }} as salarioatual,
            {{ safe_numeric('custodoservidor') }} as custodoservidor,
            email1::text as email1,
            email2::text as email2,
            email3::text as email3,
            telefone1::text as telefone1,
            telefone2::text as telefone2,
            telefone3::text as telefone3,
            {{ safe_bigint('unidadeipeaid') }} as unidadeipeaid,
            regexp_replace(residenciacep, '[^0-9]', '', 'g')::text as residenciacep,
            residenciarua::text as residenciarua,
            residencianumero::text as residencianumero,
            residenciacomplemento::text as residenciacomplemento,
            residenciabairro::text as residenciabairro,
            servidorminicv::text as servidorminicv,
            linktolattes::text as linktolattes,
            {{ safe_bigint('ufid') }} as ufid,
            {{ safe_bigint('municipioid') }} as municipioid,
            {{ safe_bigint('dasid') }} as dasid,
            {{ safe_bigint('categoriaid') }} as categoriaid,
            matriculasiape::text as matriculasiape,
            {{ safe_bigint('sexoid') }} as sexoid,
            {{ safe_bigint('statuscadastroid') }} as statuscadastroid,
            {{ safe_bigint('situacaoservidorid') }} as situacaoservidorid,
            login::text as login,
            senha::text as senha,
            {{ safe_bigint('usuarioid') }} as usuarioid,
            {{ safe_bigint('idnosistemalegado') }} as idnosistemalegado,
            {{ safe_bigint('instituicaoid') }} as instituicaoid,
            regexp_replace(trabalhocep, '[^0-9]', '', 'g')::text as trabalhocep,
            trabalhorua::text as trabalhorua,
            trabalhonumero::text as trabalhonumero,
            trabalhocomplemento::text as trabalhocomplemento,
            trabalhobairro::text as trabalhobairro,
            {{ safe_bigint('trabalhoufid') }} as trabalhoufid,
            {{ safe_bigint('trabalhomunicipioid') }} as trabalhomunicipioid,
            {{ safe_bigint('cargocomissaoid') }} as cargocomissaoid,
            ramal::text as ramal
        from {{ source("ipea_pro", "servidorespublicos") }}
    )

select *
from servidorespublicos
