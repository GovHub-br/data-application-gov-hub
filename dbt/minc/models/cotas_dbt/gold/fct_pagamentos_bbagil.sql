{{ config(enabled=false) }}
-- DESABILITADO junto com stg_bbagil até a extração bbágil rodar. enabled=true depois.
--
-- Fato de pagamentos VIA BANCO (valor PAGO ao beneficiário final) cruzado com
-- perfil raça/PCD. Espelha o fct_pagamentos_elegiveis (payment-first, LEFT JOIN,
-- preserva órfãos sem perfil) mas usa o bbágil como lado-valor — o denominador
-- CORRETO p/ cotas (pago a pessoas, não repasse a entes).
--
-- Roda em PARALELO ao fct_pagamentos_elegiveis (não o substitui ainda). O modelo
-- comparativo_recebido_vs_pago confronta os dois. A decisão de trocar o denominador
-- oficial das cotas p/ este é do lab (ver plano_integracao_meta3.md).
with pagamentos as (
    select * from {{ ref('stg_bbagil') }}
),
ente_ano as (
    select * from {{ ref('bbagil_ente_ano') }}
),
perfil as (
    select * from {{ ref('perfil_agentes_normalizado') }}
)
select
    p.identificador_unico,
    p.ente_bbagil,
    p.valor_pago_num,
    p.chave_anonimizada,
    ea.ano_plano                            as ano_final,
    'vigencia_plano'                        as origem_ano,
    ea.codigo_ibge,
    ea.municipio,
    ea.uf,
    'pnab_bbagil'                           as origem,
    (pf.identificador_unico is not null)    as tem_perfil,
    coalesce(pf.flag_negra, false)          as flag_negra,
    coalesce(pf.flag_indigena, false)       as flag_indigena,
    coalesce(pf.is_pcd, false)              as flag_pcd
from pagamentos p
left join ente_ano ea
    on p.ente_bbagil = ea.ente_bbagil
left join perfil pf
    on p.identificador_unico = pf.identificador_unico
   and p.identificador_unico is not null
