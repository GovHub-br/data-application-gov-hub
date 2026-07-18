{{ config(enabled=false) }}
-- DESABILITADO junto com stg_bbagil até a extração bbágil rodar. enabled=true depois.
--
-- Datação e localização do bbágil por ENTE (id_plano_acao). O fato_bbagil NÃO
-- traz ano nem município — busca-os em raw_planos_acao via ente_bbagil = id_plano_acao.
--
-- ano_plano = ano do INÍCIO DA VIGÊNCIA do plano de ação. É aproximação (mede o
-- ciclo do plano, não a data exata do edital), por isso origem_ano='vigencia_plano'
-- marca menor precisão. codigo_ibge destrava o cruzamento com município (e futura
-- ponte com Querido Diário, que busca por território IBGE).
with planos as (
    select
        id_plano_acao,
        codigo_ibge_municipio_ente_recebedor_plano_acao as codigo_ibge,
        nome_municipio_ente_recebedor_plano_acao        as municipio,
        uf_ente_recebedor_plano_acao                    as uf,
        data_inicio_vigencia_plano_acao                 as dt_inicio_vigencia
    from {{ source('transferegov_fundo_a_fundo', 'raw_planos_acao') }}
),
com_ano as (
    select
        id_plano_acao,
        codigo_ibge,
        municipio,
        uf,
        -- extrai o ano (AAAA) da vigência, validando faixa [2013,2026].
        -- TEXT (como ano_final do fct_pagamentos_elegiveis) p/ os dois fatos
        -- serem compatíveis e o comparativo/uniões não quebrarem por tipo.
        case
            when substring(dt_inicio_vigencia from '(20[12][0-9])') ~ '^20[12][0-9]$'
                 and substring(dt_inicio_vigencia from '(20[12][0-9])')::int between 2013 and 2026
                then substring(dt_inicio_vigencia from '(20[12][0-9])')
        end as ano_plano
    from planos
)
select
    -- ente_bbagil no fato é TEXT (id_plano_acao); casta p/ casar no join
    id_plano_acao::text as ente_bbagil,
    codigo_ibge,
    municipio,
    uf,
    ano_plano
from com_ano
