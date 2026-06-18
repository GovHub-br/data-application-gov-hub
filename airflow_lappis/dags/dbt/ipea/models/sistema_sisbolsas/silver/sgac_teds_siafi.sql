with
    sgac_teds as (
        select
            id as sgac_id,
            titulo,
            instrumento,
            numero_do_proc,
            numero_siafi,
            data_inicio,
            data_vencimento,
            diretoria_responsavel,
            fiscal_e_substituto,
            total_de_recursos,
            regexp_replace(coalesce(numero_siafi, ''), '[^0-9]', '', 'g')
            as siafi_norm
        from {{ ref("sgac_projetos_sgac") }}
        where (
            instrumento ilike '%TED%'
            or instrumento ilike '%execução descentralizada%'
            or instrumento ilike '%Dispensa de TED%'
        )
          and regexp_replace(coalesce(numero_siafi, ''), '[^0-9]', '', 'g') <> ''
    ),

    transfere_gov_teds as (
        select distinct
            coalesce(r.plano_acao, p.id_plano_acao) as plano_acao,
            r.num_transf,
            p.sq_instrumento,
            p.sigla_unidade_descentralizada,
            p.unidade_descentralizada,
            p.dt_inicio_vigencia,
            p.dt_fim_vigencia,
            p.tx_situacao_plano_acao,
            p.vl_total_plano_acao,
            r.valor_firmado,
            r.orcamento_recebido,
            r.orcamento_devolvido,
            r.empenhado,
            r.empenho_anulado,
            r.despesas_pagas_exercicio,
            r.despesas_pagas_rap,
            r.restos_a_pagar,
            r.despesas_liquidada,
            r.financeiro_recebido,
            r.financeiro_devolvido,
            r.financeiro_cancelado,
            regexp_replace(coalesce(r.num_transf, ''), '[^0-9]', '', 'g')
            as transf_norm
        from {{ ref("ted_resumo_orcamentario") }} as r
        full join {{ ref("planos_acao") }} as p
            on p.id_plano_acao = r.plano_acao
    ),

    matches as (
        select distinct
            s.sgac_id,
            s.titulo,
            s.instrumento,
            s.numero_do_proc,
            s.numero_siafi,
            s.data_inicio,
            s.data_vencimento,
            s.diretoria_responsavel,
            s.fiscal_e_substituto,
            s.total_de_recursos,
            t.plano_acao,
            t.num_transf,
            t.sq_instrumento,
            t.sigla_unidade_descentralizada,
            t.unidade_descentralizada,
            t.dt_inicio_vigencia,
            t.dt_fim_vigencia,
            t.tx_situacao_plano_acao,
            t.vl_total_plano_acao,
            t.valor_firmado,
            t.orcamento_recebido,
            t.orcamento_devolvido,
            t.empenhado,
            t.empenho_anulado,
            t.despesas_pagas_exercicio,
            t.despesas_pagas_rap,
            t.restos_a_pagar,
            t.despesas_liquidada,
            t.financeiro_recebido,
            t.financeiro_devolvido,
            t.financeiro_cancelado,
            'numero_siafi' as chave_match,
            'SGAC.numero_siafi -> SIAFI.num_transf' as caminho_match
        from sgac_teds as s
        inner join transfere_gov_teds as t
            on length(s.siafi_norm) >= 5
           and s.siafi_norm = t.transf_norm
    )

select
    sgac_id,
    titulo,
    instrumento,
    numero_do_proc,
    numero_siafi,
    data_inicio,
    data_vencimento,
    diretoria_responsavel,
    fiscal_e_substituto,
    total_de_recursos,
    plano_acao,
    num_transf,
    sq_instrumento,
    sigla_unidade_descentralizada,
    unidade_descentralizada,
    dt_inicio_vigencia,
    dt_fim_vigencia,
    tx_situacao_plano_acao,
    vl_total_plano_acao,
    valor_firmado,
    orcamento_recebido,
    orcamento_devolvido,
    empenhado,
    empenho_anulado,
    despesas_pagas_exercicio,
    despesas_pagas_rap,
    restos_a_pagar,
    despesas_liquidada,
    financeiro_recebido,
    financeiro_devolvido,
    financeiro_cancelado,
    chave_match,
    caminho_match
from matches
