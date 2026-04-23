{{ config(materialized="table") }}

with
    bolsistas as (
        select
            {{ safe_bigint('bolsistaid') }} as bolsistaid,
            {{ safe_bigint('categoriaadicionalbolsistaid') }} as categoriaadicionalbolsistaid,
            nomebolsista::text as nomebolsista,
            {{ safe_date('datanascimento') }} as datanascimento,
            regexp_replace(numerocpf, '[^0-9]', '', 'g')::text as numerocpf,
            numerorg::text as numerorg,
            expedidorrg::text as expedidorrg,
            {{ safe_bigint('coordenadorid') }} as coordenadorid,
            {{ safe_bigint('diretoriaid') }} as diretoriaid,
            {{ safe_bigint('coordenacaoid') }} as coordenacaoid,
            {{ safe_bigint('unidadeipeaid') }} as unidadeipeaid,
            email1::text as email1,
            email2::text as email2,
            email3::text as email3,
            telefone1::text as telefone1,
            telefone2::text as telefone2,
            telefone3::text as telefone3,
            regexp_replace(residenciacep, '[^0-9]', '', 'g')::text as residenciacep,
            residenciarua::text as residenciarua,
            residencianumero::text as residencianumero,
            residenciacomplemento::text as residenciacomplemento,
            residenciabairro::text as residenciabairro,
            bolsistaminicv::text as bolsistaminicv,
            linktolattes::text as linktolattes,
            {{ safe_bigint('ufid') }} as ufid,
            {{ safe_bigint('municipioid') }} as municipioid,
            {{ safe_date('bolsadatainicio') }} as bolsadatainicio,
            {{ safe_date('bolsadatafinalizacao') }} as bolsadatafinalizacao,
            {{ safe_bigint('categoriabolsistaid') }} as categoriabolsistaid,
            {{ safe_bigint('projetoid') }} as projetoid,
            {{ safe_bigint('sexoid') }} as sexoid,
            {{ safe_bigint('situacaopessoaid') }} as situacaopessoaid,
            {{ safe_bigint('statuscadastroid') }} as statuscadastroid,
            {{ safe_bigint('custoemprojetoid') }} as custoemprojetoid,
            login::text as login,
            senha::text as senha,
            {{ safe_bigint('usuarioid') }} as usuarioid,
            {{ safe_bigint('idnosistemalegado') }} as idnosistemalegado,
            matriculaipea::text as matriculaipea,
            observacoes::text as observacoes
        from {{ source("ipea_pro", "bolsistas") }}
    )

select *
from bolsistas
