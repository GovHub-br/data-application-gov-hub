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
        regexp_replace(coalesce(numero_do_proc, ''), '[^0-9]', '', 'g') as proc_norm,
        regexp_replace(coalesce(numero_siafi, ''), '[^0-9]', '', 'g') as siafi_norm
    from sistema_sisbolsas.sgac_projetos_sgac
    where instrumento ilike '%TED%'
       or instrumento ilike '%execução descentralizada%'
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
        regexp_replace(
            coalesce(p.sq_instrumento, ''), '[^0-9]', '', 'g'
        ) as sq_norm,
        regexp_replace(coalesce(r.num_transf, ''), '[^0-9]', '', 'g') as transf_norm
    from {{ ref("ted_resumo_orcamentario") }} r
    full join {{ ref("planos_acao") }} p
        on p.id_plano_acao = r.plano_acao
),

siafi_empenhos as (
    select distinct
        plano_acao,
        num_transf,
        regexp_replace(coalesce(ne_num_processo, ''), '[^0-9]', '', 'g') as proc_norm
    from {{ ref("empenhos_plano_acao") }}
    where plano_acao is not null
       or num_transf is not null
       or ne_num_processo is not null
),

siafi_ted_keys as (
    select distinct plano_acao, num_transf
    from siafi_empenhos
),

match_numero_siafi as (
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
        'SGAC.numero_siafi -> TransfereGov/SIAFI -> SIAFI.empenhos_plano_acao' as caminho_match,
        true as match_transfere_gov,
        sk.plano_acao is not null or sk.num_transf is not null as match_siafi_empenho,
        1 as prioridade_match
    from sgac_teds s
    inner join transfere_gov_teds t
        on s.siafi_norm <> ''
       and length(s.siafi_norm) >= 5
       and (
            s.siafi_norm = t.sq_norm
            or s.siafi_norm = t.transf_norm
       )
    left join siafi_ted_keys sk
        on (
            t.plano_acao is not null
            and sk.plano_acao = t.plano_acao
        )
        or (
            t.num_transf is not null
            and sk.num_transf = t.num_transf
        )
),

match_numero_proc_sei as (
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
        coalesce(t.plano_acao, e.plano_acao) as plano_acao,
        coalesce(t.num_transf, e.num_transf) as num_transf,
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
        'numero_do_proc' as chave_match,
        'SGAC.numero_do_proc -> SIAFI.empenhos_plano_acao -> TransfereGov/SIAFI' as caminho_match,
        t.plano_acao is not null or t.num_transf is not null as match_transfere_gov,
        true as match_siafi_empenho,
        2 as prioridade_match
    from sgac_teds s
    inner join siafi_empenhos e
        on s.proc_norm <> ''
       and s.proc_norm = e.proc_norm
    left join transfere_gov_teds t
        on (
            e.plano_acao is not null
            and t.plano_acao = e.plano_acao
        )
        or (
            e.num_transf is not null
            and t.num_transf = e.num_transf
        )
),

matches as (
    select * from match_numero_siafi
    union all
    select * from match_numero_proc_sei
),

matches_priorizados as (
    select
        *,
        row_number() over (
            partition by sgac_id, coalesce(plano_acao, ''), coalesce(num_transf, '')
            order by prioridade_match
        ) as ordem_match
    from matches
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
    caminho_match,
    match_transfere_gov,
    match_siafi_empenho
from matches_priorizados
where ordem_match = 1
