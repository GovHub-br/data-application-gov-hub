{{ config(materialized="table") }}

with
    emendas as (select * from {{ ref("numero_transferencia") }}),
    conv as (select * from {{ ref("proposta_convenio") }}),
    ted as (
        select * from {{ ref("ted_resumo_orcamentario") }} where plano_acao is not null
    )

select
    e.emissao_mes,
    e.emissao_dia,
    e.codigo_programa,
    e.programa,
    e.codigo_acao_ajustada,
    e.acao_ajustada,
    e.autor_emendas_orcamento_descricao,
    e.autor_emendas_orcamento_nome,
    e.localizador_gasto,
    e.localizador_gasto_descricao,
    e.regiao_pt,
    e.uf,
    e.uf_descricao,
    e.municipio,
    e.pais,
    e.ne_ccor,
    e.ne_num_processo,
    e.ne_info_complementar,
    e.ne_ccor_descricao,
    e.doc_observacao,
    e.codigo_gnd,
    e.gnd,
    e.natureza_despesa,
    e.natureza_despesa_descricao,
    e.codigo_modalidade,
    e.modalidade,
    e.ne_ccor_favorecido,
    e.ne_ccor_favorecido_descricao,
    e.ne_ccor_ano_emissao,
    e.ptres,
    e.fonte_recursos_detalhada,
    e.fonte_recursos_detalhada_descricao,
    e.despesas_empenhadas,
    e.despesas_liquidadas,
    e.despesas_pagas,
    e.restos_a_pagar_inscritos,
    e.restos_a_pagar_pagos,
    e.id_autor,
    e.cargo_autor,
    e.autor,
    e.partido,
    e.uf_autor,
    e.url_foto_autor,
    e.email_autor,
    e.url_foto_partido,
    e.numero_transferencia,
    e.dt_ingest,

    -- Instrumento: identificacao unificada
    case
        when ted.num_transf is not null then 'TED' else conv.modalidade
    end as tipo_instrumento,
    e.numero_transferencia as numero_instrumento,
    coalesce(conv.objeto_proposta, ted.tx_objetivo_programa) as objeto_instrumento,

    -- Instrumento: localizacao/executor
    coalesce(
        concat(
            conv.munic_proponente, ' - ', conv.uf_proponente, ' : ', conv.nm_proponente
        ),
        ted.sigla_unidade_descentralizada
    ) as beneficiario,

    -- Instrumento: programa (TED)
    ted.tx_nome_institucional_programa as nome_programa_ted,
    ted.programa_governo,
    ted.programa_governo_descricao,

    -- Valores: firmado
    coalesce(conv.vl_global_conv, ted.valor_firmado) as valor_firmado

from emendas e
left join conv on e.numero_transferencia = conv.nr_convenio
left join ted on e.numero_transferencia::text = ted.num_transf
