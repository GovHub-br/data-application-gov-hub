with
    bolsistas as (
        select
            b.co_usuario,
            b.co_selecao,
            b.nu_bolsa,
            b.co_situacao_bolsista,
            case
                when b.dt_inicio ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(b.dt_inicio from 1 for 10)::date
            end as dt_inicio,
            case
                when b.dt_fim ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(b.dt_fim from 1 for 10)::date
            end as dt_fim
        from {{ ref("sisbolsas_tb_bolsista") }} as b
    ),

    usuarios as (
        select
            u.co_usuario,
            u.ds_nome,
            regexp_replace(u.ds_login, '[^0-9]', '', 'g') as ds_login_cpf
        from {{ ref("sisbolsas_tb_usuario") }} as u
    ),

    selecoes as (
        select s.co_selecao, s.co_chamada_publica, s.co_modalidade, s.tp_atuacao
        from {{ ref("sisbolsas_tb_selecao") }} as s
    ),

    chamadas as (
        select cp.co_chamada_publica, cp.co_programa, cp.co_projeto, cp.tp_moeda
        from {{ ref("sisbolsas_tb_chamada_publica") }} as cp
    ),

    unidade_por_chamada as (
        select
            cu.co_chamada_publica,
            string_agg(distinct u.ds_sigla, ' | ' order by u.ds_sigla) as unidade,
            string_agg(distinct e.ds_uf, ' | ' order by e.ds_uf) as uf_unidade
        from {{ ref("sisbolsas_tb_chapubli_unidade") }} as cu
        left join {{ ref("sisbolsas_tb_unidade") }} as u on cu.co_unidade = u.co_unidade
        left join {{ ref("sisbolsas_tb_estado") }} as e on u.co_estado = e.co_estado
        group by 1
    ),

    pagamentos as (
        select
            fb.co_usuario,
            fb.co_fonte_financeira,
            fb.vl_total_pago,
            fp.nu_mes,
            fp.nu_ano,
            case
                when fp.nu_ano ~ '^[0-9]{4}$' and fp.nu_mes ~ '^[0-9]{1,2}$'
                then
                    to_date(
                        fp.nu_ano || '-' || lpad(fp.nu_mes, 2, '0') || '-01', 'YYYY-MM-DD'
                    )
            end as mes_referencia
        from {{ ref("sisbolsas_tb_folha_bolsista") }} as fb
        left join
            {{ ref("sisbolsas_tb_folha_pagamento") }} as fp
            on fb.co_folha_pagamento = fp.co_folha_pagamento
    ),

    coordenador_por_selecao as (
        select
            bc.co_selecao,
            string_agg(
                distinct coalesce(uc.ds_nome, bc.ds_cpf),
                ' | '
                order by coalesce(uc.ds_nome, bc.ds_cpf)
            ) as coordenador
        from {{ ref("sisbolsas_tb_bolsa_coordenador") }} as bc
        left join
            usuarios as uc
            on regexp_replace(bc.ds_cpf, '[^0-9]', '', 'g') = uc.ds_login_cpf
        group by 1
    )

select
    uc.unidade as unidade,
    ub.ds_nome as bolsista,
    proj.tituloprojeto as titulo_projeto,
    prog.ds_programa as programa,
    mod.ds_modalidade as modalidade,
    b.dt_inicio as inicio,
    b.dt_fim as termino,
    uc.uf_unidade,
    p.vl_total_pago as valor,
    ff.ds_fonte_financeira as recurso,
    coord.coordenador,
    p.mes_referencia,
    b.co_situacao_bolsista as situacao_bolsista,
    case
        when s.tp_atuacao = '1'
        then 'Presencial'
        when s.tp_atuacao = '2'
        then 'Não presencial'
    end as atividade,
    case
        when c.tp_moeda = '1'
        then 'Real (R$)'
        when c.tp_moeda is null
        then null
        else 'Estrangeira'
    end as moeda,
    case
        when p.nu_mes ~ '^[0-9]{1,2}$' then p.nu_mes::integer
    end as mes_referencia_numero,
    case when p.nu_ano ~ '^[0-9]{4}$' then p.nu_ano::integer end as ano_referencia
from bolsistas as b
left join usuarios as ub on b.co_usuario = ub.co_usuario
left join selecoes as s on b.co_selecao = s.co_selecao
left join chamadas as c on s.co_chamada_publica = c.co_chamada_publica
left join unidade_por_chamada as uc on c.co_chamada_publica = uc.co_chamada_publica
left join {{ ref("sisbolsas_tb_programa") }} as prog on c.co_programa = prog.co_programa
left join
    {{ ref("sisbolsas_tb_modalidade") }} as mod on s.co_modalidade = mod.co_modalidade
left join pagamentos as p on b.co_usuario = p.co_usuario
left join
    {{ ref("sisbolsas_tb_fonte_financeira") }} as ff
    on p.co_fonte_financeira = ff.co_fonte_financeira
left join coordenador_por_selecao as coord on b.co_selecao = coord.co_selecao
left join {{ ref("ipea_pro_projetos") }} as proj on proj.projetoid::text = c.co_projeto
