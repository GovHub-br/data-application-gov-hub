with
    total_servidores as (
        select 
            count(distinct cpf) as total,
            min(dt_ingest) as dt_ingest
        from {{ ref("dados_funcionais") }}
    ),

    servidores_ativos as (
        select 
            count(distinct cpf) as total,
            min(dt_ingest) as dt_ingest
        from {{ ref("dados_funcionais") }}
        where nome_situacao_funcional in ('ATIVO PERMANENTE')
    ),

    aposentados as (
        select 
            count(distinct cpf) as total, 
            min(dt_ingest) as dt_ingest
        from {{ ref("dados_funcionais") }}
        where nome_situacao_funcional in ('APOSENTADO')
    ),

    estagiarios as (
        select 
            count(distinct cpf) as total, 
            min(dt_ingest) as dt_ingest
        from {{ ref("dados_funcionais") }}
        where nome_situacao_funcional in ('ESTAGIARIO SIGEPE')
    ),

    terceirizados as (
        select 
            count(distinct id) as total,
            min(dt_ingest) as dt_ingest
        from {{ ref("terceirizados") }}
    )

select 
    'total_servidores' as kpi, 
    total as valor,
    dt_ingest,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from total_servidores

union all

select 
    'servidores_ativos_permanentes' as kpi, 
    total as valor,
    dt_ingest,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from servidores_ativos

union all

select 
    'aposentados' as kpi, 
    total as valor,
    dt_ingest,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from aposentados

union all

select 
    'estagiarios' as kpi, 
    total as valor,
    dt_ingest,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from estagiarios

union all

select 
    'terceirizados' as kpi, 
    total as valor,
    dt_ingest,
    {{ brasilia_now_iso() }}::timestamptz as dt_transform
from terceirizados
